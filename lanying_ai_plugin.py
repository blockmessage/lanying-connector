import logging
import time
import lanying_redis
import json
import lanying_embedding
from urllib.parse import urlparse
from urllib.parse import urlunparse
import lanying_config
import random

def configure_ai_plugin_embedding(app_id, embedding_max_tokens, embedding_max_blocks, vendor):
    embedding_name = maybe_create_function_embedding(app_id)
    admin_user_ids = []
    preset_name = ''
    embedding_content = ''
    new_embedding_name = ''
    max_block_size = 500
    overlapping_size = 0
    old_embedding_info = get_ai_plugin_embedding(app_id)
    lanying_embedding.configure_embedding(app_id, embedding_name, admin_user_ids, preset_name, embedding_max_tokens, embedding_max_blocks, embedding_content, new_embedding_name, max_block_size, overlapping_size, vendor)
    new_embedding_info = get_ai_plugin_embedding(app_id)
    if old_embedding_info.get('vendor', 'openai') != new_embedding_info.get('vendor', 'openai'):
        redis = lanying_redis.get_redis_connection()
        list_key = get_ai_plugin_ids_key(app_id)
        plugin_ids = lanying_redis.redis_lrange(redis, list_key, 0, -1)
        from lanying_tasks import process_function_embeddings
        for plugin_id in plugin_ids:
            function_ids = list_ai_function_ids(app_id, plugin_id)
            if len(function_ids) > 0:
                process_function_embeddings.apply_async(args = [app_id, plugin_id, function_ids])
    return {'result': 'ok', 'data':{'success': True}}

def get_ai_plugin_embedding(app_id):
    embedding_name = maybe_create_function_embedding(app_id)
    details = lanying_embedding.get_embedding_info_with_details(app_id, embedding_name)
    ret = {}
    if details:
        for key in [ "embedding_max_tokens", "embedding_max_blocks",  "vendor"]:
            if key in details:
                ret[key] = details[key]
    return ret

def create_ai_plugin(app_id, plugin_name):
    logging.info(f"start create ai plugin: app_id:{app_id}, plugin_name:{plugin_name}")
    now = int(time.time())
    embedding_name = maybe_create_function_embedding(app_id)
    embedding_name_info = lanying_embedding.get_embedding_name_info(app_id, embedding_name)
    embedding_uuid = embedding_name_info['embedding_uuid']
    doc_id = lanying_embedding.generate_doc_id(embedding_uuid)
    redis = lanying_redis.get_redis_connection()
    plugin_id = generate_ai_plugin_id()
    lanying_embedding.create_doc_info(app_id, embedding_uuid, f'ai_plugin_{plugin_id}', f'dummy_object_name_{plugin_id}', doc_id, 0, '.plugin', 'plugin', f'dummy_url_{plugin_id}', "openai",{})
    set_doc_id_to_plugin_id(app_id, doc_id, plugin_id)
    redis.hmset(get_ai_plugin_key(app_id, plugin_id), {
        "app_id": app_id,
        "name": plugin_name,
        "plugin_id": plugin_id,
        "create_time": now,
        "embedding_name": embedding_name,
        "embedding_uuid": embedding_uuid,
        "doc_id": doc_id,
        "headers": "{}",
        "endpoint":""
    })
    redis.rpush(get_ai_plugin_ids_key(app_id), plugin_id)
    return {'result':'ok', 'data':{'id':plugin_id}}

def list_ai_plugins(app_id):
    redis = lanying_redis.get_redis_connection()
    list_key = get_ai_plugin_ids_key(app_id)
    plugin_ids = lanying_redis.redis_lrange(redis, list_key, 0, -1)
    result = []
    for plugin_id in plugin_ids:
        info = get_ai_plugin(app_id, plugin_id)
        if info:
            dto = {}
            for key in ["name", "plugin_id", "create_time", "endpoint", "envs", "headers", "params"]:
                if key in info:
                    if key in ["envs", "headers", "params"]:
                        dto[key] = json.loads(info[key])
                    else:
                        dto[key] = info[key]
                elif key in ["envs", "headers", "params"]:
                    dto[key] = {}
            result.append(dto)
    return {'result':'ok', 'data':{'list': result}}

def list_ai_functions(app_id, plugin_id, start, end):
    redis = lanying_redis.get_redis_connection()
    list_key = get_ai_function_ids_key(app_id, plugin_id)
    function_ids = lanying_redis.redis_lrange(redis, list_key, start, end)
    result = []
    total = redis.llen(list_key)
    for function_id in function_ids:
        info = get_ai_function(app_id, function_id)
        if info:
            dto = {}
            for key in ["function_id", "plugin_id", "create_time", "name", "description", "parameters", "function_call"]:
                if key in info:
                    if key in ["parameters", "function_call"]:
                        dto[key] = json.loads(info[key])
                    else:
                        dto[key] = info[key]
            result.append(dto)
    return {'result':'ok', 'data':{'list': result, 'total': total}}

def list_ai_function_ids(app_id, plugin_id):
    redis = lanying_redis.get_redis_connection()
    list_key = get_ai_function_ids_key(app_id, plugin_id)
    return lanying_redis.redis_lrange(redis, list_key, 0, -1)

def get_ai_plugin(app_id, plugin_id):
    redis = lanying_redis.get_redis_connection()
    key = get_ai_plugin_key(app_id, plugin_id)
    info = lanying_redis.redis_hgetall(redis, key)
    if "create_time" in info:
        return info
    return None

def get_ai_function(app_id, function_id):
    redis = lanying_redis.get_redis_connection()
    key = get_ai_function_key(app_id, function_id)
    info = lanying_redis.redis_hgetall(redis, key)
    if "create_time" in info:
        return info
    return None

def add_ai_function_to_ai_plugin(app_id, plugin_id, name, description, parameters, function_call):
    plugin = get_ai_plugin(app_id, plugin_id)
    if not plugin:
        return {'result': 'error', 'message': 'ai plugin not exist'}
    function_num_limit = lanying_config.get_lanying_connector_function_num_limit(app_id)
    function_num = get_ai_function_count(app_id)
    if function_num >= function_num_limit:
        return {'result': 'error', 'message': 'ai function num limit exceed'}
    now = int(time.time())
    embedding_uuid = plugin["embedding_uuid"]
    doc_id = plugin["doc_id"]
    block_id = lanying_embedding.generate_block_id(embedding_uuid, doc_id)
    function_id = generate_ai_function_id(app_id, plugin_id)
    ai_function_info = {
        'function_id': function_id,
        'plugin_id': plugin_id,
        'block_id': block_id,
        "create_time": now,
        'name': name,
        'description': description,
        'parameters': json.dumps(parameters, ensure_ascii=False),
        'function_call': json.dumps(function_call, ensure_ascii=False)
    }
    redis = lanying_redis.get_redis_connection()
    increase_ai_function_count(app_id, 1)
    redis.hmset(get_ai_function_key(app_id, function_id), ai_function_info)
    redis.rpush(get_ai_function_ids_key(app_id, plugin_id), function_id)
    from lanying_tasks import process_function_embeddings
    process_function_embeddings.apply_async(args = [app_id, plugin_id, [function_id]])
    return {'result':'ok', 'data':{'function_id': function_id}}

def process_function_embedding(app_id, plugin_id, function_id):
    ai_plugin_info = get_ai_plugin(app_id, plugin_id)
    if not ai_plugin_info:
        return {'result':'error', 'message': 'ai plugin not exist'}
    ai_function_info = get_ai_function(app_id, function_id)
    if not ai_function_info:
        return {'result':'error', 'message': 'ai function not exist'}
    redis_stack = lanying_redis.get_redis_stack_connection()
    embedding_uuid = ai_plugin_info["embedding_uuid"]
    doc_id = ai_plugin_info["doc_id"]
    embedding_uuid_info = lanying_embedding.get_embedding_uuid_info(embedding_uuid)
    name = ai_function_info['name']
    embedding_name = ai_plugin_info["embedding_name"]
    description = ai_function_info['description']
    block_id = ai_function_info['block_id']
    parameters = safe_json_loads(ai_function_info.get('parameters', '{}'))
    function_call = safe_json_loads(ai_function_info.get('function_call','{}'))
    function_info = {
        'name': name,
        'description': description,
        'parameters': parameters,
        'function_call': function_call
    }
    function = json.dumps(function_info, ensure_ascii=False)
    text = description
    token_cnt = lanying_embedding.num_of_tokens(function)
    blocks = [(token_cnt, "function", text, function, block_id)]
    lanying_embedding.delete_embedding_block(app_id, embedding_name, doc_id, block_id)
    lanying_embedding.insert_embeddings(embedding_uuid_info, app_id, embedding_uuid,"function", doc_id, blocks, redis_stack)

def fill_parameters_to_function_call(function_call, parameters):
    method = function_call.get('method', 'get')
    params = function_call.get('params', {})
    headers = function_call.get('headers', {})
    body = function_call.get('body', {})
    logging.info(f"processing function: start, function_call:{function_call}, parameters:{parameters}")
    for property,_ in parameters.get('properties',{}).items():
        if property not in headers and property not in params and property not in body:
            if method == 'get':
                params[property] = {
                    "type": "variable",
                    "value": property
                }
            else:
                body[property] = {
                    "type": "variable",
                    "value": property
                }
    function_call['params'] = params
    function_call['body'] = body
    logging.info(f"processing function: finish, function_call:{function_call}")
    return function_call

def delete_ai_function_from_ai_plugin(app_id, plugin_id, function_id):
    plugin = get_ai_plugin(app_id, plugin_id)
    if not plugin:
        return {'result': 'error', 'message': 'ai plugin not exist'}
    ai_function_info = get_ai_function(app_id, function_id)
    if not ai_function_info:
        return {'result':'error', 'message': 'ai function not exist'}
    doc_id = plugin["doc_id"]
    embedding_name = plugin["embedding_name"]
    block_id = ai_function_info['block_id']
    redis = lanying_redis.get_redis_connection()
    lanying_embedding.delete_embedding_block(app_id, embedding_name, doc_id, block_id)
    redis.lrem(get_ai_function_ids_key(app_id, plugin_id), 1, function_id)
    redis.delete(get_ai_function_key(app_id, function_id))
    increase_ai_function_count(app_id, -1)
    return {'result':'ok', 'data':{'success': True}}

def configure_ai_plugin(app_id, plugin_id, name, endpoint, headers, envs, params):
    ai_plugin_info = get_ai_plugin(app_id, plugin_id)
    if not ai_plugin_info:
        return {'result':'error', 'message': 'ai plugin not exist'}
    redis = lanying_redis.get_redis_connection()
    redis.hmset(get_ai_plugin_key(app_id, plugin_id), {
        'name': name,
        'headers': json.dumps(headers, ensure_ascii=False),
        'params': json.dumps(params, ensure_ascii=False),
        'envs': json.dumps(envs, ensure_ascii=False),
        'endpoint': endpoint
    })
    return {'result':'ok', 'data':{'success': True}}

def configure_ai_function(app_id, plugin_id, function_id, name, description, parameters,function_call):
    ai_plugin_info = get_ai_plugin(app_id, plugin_id)
    if not ai_plugin_info:
        return {'result':'error', 'message': 'ai plugin not exist'}
    ai_function_info = get_ai_function(app_id, function_id)
    if not ai_function_info:
        return {'result':'error', 'message': 'ai function not exist'}
    redis = lanying_redis.get_redis_connection()
    redis.hmset(get_ai_function_key(app_id, function_id), {
        'name': name,
        'description': description,
        'parameters': json.dumps(parameters, ensure_ascii=False),
        'function_call': json.dumps(function_call, ensure_ascii=False)
    })
    from lanying_tasks import process_function_embeddings
    process_function_embeddings.apply_async(args = [app_id, plugin_id, [function_id]])
    return {'result':'ok', 'data':{'success': True}}

def bind_ai_plugin(app_id, type, name, list):
    if type == 'plugin_list':
        preset_names = lanying_embedding.get_preset_names(app_id)
        if name not in preset_names:
            return {'result':'error', 'message': 'preset_name not exist'}
        relation = get_ai_plugin_bind_relation(app_id)
        plugin_id_list = []
        for plugin_id in list:
            ai_plugin_info = get_ai_plugin(app_id, plugin_id)
            if ai_plugin_info:
                plugin_id_list.append(plugin_id)
        relation[name] = plugin_id_list
        set_ai_plugin_bind_relation(app_id, relation)
        return {'result':'ok', 'data':{}}
    elif type == "preset_name_list":
        plugin_id = name
        ai_plugin_info = get_ai_plugin(app_id, plugin_id)
        if not ai_plugin_info:
            return {'result':'error', 'message':'plugin_id not exist'}
        relation = get_ai_plugin_bind_relation(app_id)
        preset_names = lanying_embedding.get_preset_names(app_id)
        for preset_name in preset_names:
            if preset_name in list:
                if preset_name in relation:
                    if plugin_id not in relation[preset_name]:
                        relation[preset_name].append(plugin_id)
                else:
                    relation[preset_name] = [plugin_id]
            else:
                if preset_name in relation:
                    if plugin_id in relation[preset_name]:
                        relation[preset_name].remove(plugin_id)
        set_ai_plugin_bind_relation(app_id, relation)
        return {'result':'ok', 'data':{}}
    else:
        return {'result':'error', 'message':'bad argument: type'}

def get_ai_plugin_bind_relation(app_id):
    key = ai_plugin_bind_relation_key(app_id)
    redis = lanying_redis.get_redis_connection()
    str = lanying_redis.redis_get(redis, key)
    if str:
        return json.loads(str)
    return {}

def set_ai_plugin_bind_relation(app_id, relation):
    key = ai_plugin_bind_relation_key(app_id)
    redis = lanying_redis.get_redis_connection()
    redis.set(key, json.dumps(relation, ensure_ascii=False))

def get_preset_function_embeddings(app_id, preset_name):
    relation = get_ai_plugin_bind_relation(app_id)
    plugin_ids = relation.get(preset_name, [])
    doc_ids = []
    for plugin_id in plugin_ids:
        plugin_info = get_ai_plugin(app_id, plugin_id)
        if plugin_info:
            doc_id = plugin_info['doc_id']
            doc_ids.append(doc_id)
    if len(doc_ids) == 0:
        return []
    embedding_name = get_function_embedding_name(app_id)
    if not embedding_name:
        return []
    embedding_info = lanying_embedding.get_embedding_name_info(app_id, embedding_name)
    embedding_info["embedding_max_tokens"] = int(embedding_info.get("embedding_max_tokens","2048"))
    embedding_info["embedding_max_blocks"] = int(embedding_info.get("embedding_max_blocks", "5"))
    embedding_info["doc_ids"] = doc_ids
    return [embedding_info]

def ai_plugin_bind_relation_key(app_id):
    return f"lanying_connector:ai_plugin_bind_relation:{app_id}"

def generate_ai_plugin_id():
    redis = lanying_redis.get_redis_connection()
    return redis.incrby("lanying_connector:ai_plugin_id_generator", 1)

def generate_ai_function_id(app_id, plugin_id):
    redis = lanying_redis.get_redis_connection()
    id = redis.incrby(f"lanying_connector:ai_function_id_generator:{app_id}:{plugin_id}", 1)
    return f"{plugin_id}-{id}"

def get_ai_plugin_key(app_id, plugin_id):
    return f"lanying_connector:ai_plugin:{app_id}:{plugin_id}"

def get_ai_plugin_ids_key(app_id):
    return f"lanying_connector:ai_plugin_ids:{app_id}"

def get_ai_function_ids_key(app_id, plugin_id):
    return f"lanying_connector:ai_function_ids:{app_id}:{plugin_id}"

def get_ai_function_key(app_id, function_id):
    return f"lanying_connector:ai_function:{app_id}:{function_id}"

def get_function_embedding_name_key(app_id):
    return f"lanying_connector:function_embedding_name:{app_id}"

def get_ai_function_count_key(app_id):
    return f"lanying_connector:ai_function_count:{app_id}"

def get_ai_function_count(app_id):
    return increase_ai_function_count(app_id, 0)

def increase_ai_function_count(app_id, value):
    key = get_ai_function_count_key(app_id)
    redis = lanying_redis.get_redis_connection()
    return redis.incrby(key, value)

def get_function_embedding_name(app_id):
    redis = lanying_redis.get_redis_connection()
    function_embedding_name_key = get_function_embedding_name_key(app_id)
    return lanying_redis.redis_get(redis, function_embedding_name_key)

def set_function_embedding_name(app_id, embedding_name):
    redis = lanying_redis.get_redis_connection()
    function_embedding_name_key = get_function_embedding_name_key(app_id)
    redis.set(function_embedding_name_key, embedding_name)

def maybe_create_function_embedding(app_id):
    embedding_name = get_function_embedding_name(app_id)
    if not embedding_name:
        embedding_name = f"function_embedding_{int(time.time())}_{random.randint(1,100000000)}"
        lanying_embedding.create_embedding(app_id, embedding_name, 500, 'COSINE', [], "", 0, "openai", "function")
        set_function_embedding_name(app_id, embedding_name)
    return embedding_name

def set_doc_id_to_plugin_id(app_id, doc_id, plugin_id):
    key = doc_id_to_plugin_id_key(app_id)
    redis = lanying_redis.get_redis_connection()
    redis.hset(key, doc_id, plugin_id)

def get_plugin_id_by_doc_id(app_id, doc_id):
    key = doc_id_to_plugin_id_key(app_id)
    redis = lanying_redis.get_redis_connection()
    return lanying_redis.redis_hget(redis, key, doc_id)

def doc_id_to_plugin_id_key(app_id):
    return f"lanying_connector:doc_id_to_plugin_id:{app_id}"

def fill_function_info(app_id, function_info, doc_id, system_envs):
    if doc_id == '':
        logging.info("fill_function_info: empty doc_id")
        return function_info
    plugin_id = get_plugin_id_by_doc_id(app_id, doc_id)
    if not plugin_id:
        logging.info("fill_function_info: fail to get plugin_id")
        return function_info
    plugin_info = get_ai_plugin(app_id, plugin_id)
    if not plugin_info:
        logging.info("fill_function_info: fail to get plugin_info")
        return function_info
    headers = safe_json_loads(plugin_info.get('headers', '{}'))
    params = safe_json_loads(plugin_info.get('params', '{}'))
    envs = safe_json_loads(plugin_info.get('envs', '{}'))
    function_call = function_info.get('function_call', {})
    parameters = function_info.get('parameters', {})
    function_call = fill_parameters_to_function_call(function_call, parameters)
    function_call_headers = function_call.get('headers', {})
    function_call_params = function_call.get('params', {})
    function_call_body = function_call.get('body', {})
    endpoint = plugin_info.get('endpoint', '')
    if len(headers) > 0:
        for k,v in headers:
            function_call_headers[k] = v
    if len(params) > 0:
        for k,v in params:
            function_call_params[k] = v
    if len(endpoint) > 0:
        function_call_url = function_call.get('url', '')
        old_urlparse = urlparse(function_call_url)
        if len(old_urlparse.netloc) == 0:
            new_urlparse = urlparse(endpoint)
            new_url = urlunparse(old_urlparse._replace(netloc=new_urlparse.netloc,scheme=new_urlparse.scheme))
            function_call["url"] = new_url
    function_call['headers'] = fill_function_sys_envs(system_envs, fill_function_envs(envs, function_call_headers))
    function_call['params'] = fill_function_sys_envs(system_envs, fill_function_envs(envs, function_call_params))
    function_call['body'] = fill_function_sys_envs(system_envs, fill_function_envs(envs, function_call_body))
    function_info["function_call"] = function_call
    logging.info(f"function_info:{function_info}")
    return function_info

def fill_function_envs(envs, obj):
    if isinstance(obj, list):
        ret = []
        for item in obj:
            new_item = fill_function_envs(envs, item)
            ret.append(new_item)
        return ret
    elif isinstance(obj, dict):
        if ('type' in obj and obj['type'] == 'env' and 'value' in obj):
            env_name = obj['value']
            if env_name in envs:
                return envs[env_name].get('value', '')
            else:
                logging.info(f"fill_function_envs | env not found: {env_name}")
                return ''
        else:
            ret = {}
            for k,v in obj.items():
                ret[k] = fill_function_envs(envs, v)
            return ret
    else:
        return obj

def fill_function_sys_envs(envs, obj):
    if isinstance(obj, list):
        ret = []
        for item in obj:
            new_item = fill_function_sys_envs(envs, item)
            ret.append(new_item)
        return ret
    elif isinstance(obj, dict):
        if ('type' in obj and obj['type'] == 'system_env' and 'value' in obj):
            env_name = obj['value']
            if env_name in envs:
                return envs[env_name].get('value', '')
            else:
                logging.info(f"fill_function_sys_envs | env not found: {env_name}")
                return ''
        else:
            ret = {}
            for k,v in obj.items():
                ret[k] = fill_function_sys_envs(envs, v)
            return ret
    else:
        return obj

def safe_json_loads(str, default={}):
    try:
        return json.loads(str)
    except Exception as e:
        return default

def plugin_export(app_id, plugin_id):
    plugin_info = get_ai_plugin(app_id, plugin_id)
    if not plugin_info:
        return {'result':'error', 'message': 'plugin not exist'}
    function_ids = list_ai_function_ids(app_id, plugin_id)
    function_dtos = []
    for function_id in function_ids:
        function_info = get_ai_function(app_id, function_id)
        if function_info:
            function_dto = {
                'type': 'ai_function',
                'name': function_info['name'],
                'description': function_info['description'],
                'parameters': safe_json_loads(function_info['parameters']),
                'function_call': safe_json_loads(function_info['function_call'])
            }
            function_dtos.append(function_dto)
    plugin_dto = {
        'type': 'ai_plugin',
        'version': 1,
        "name": plugin_info['name'],
        "headers": safe_json_loads(plugin_info.get('headers', '{}')),
        "params": safe_json_loads(plugin_info.get('params', '{}')),
        "envs": safe_json_loads(plugin_info.get('envs', '{}')),
        "endpoint": plugin_info['endpoint'],
        "functions": function_dtos
    }
    filename = f"lanying-ai-plugin-{plugin_id}.json"
    content = json.dumps(plugin_dto, ensure_ascii=False, indent=2)
    return {'result': 'ok', 'data':{'file':{'name':filename, 'content':content}}}

def plugin_import(app_id, plugin_config):
    if plugin_config['type'] != 'ai_plugin' or plugin_config['version'] != 1:
        {'result': 'error', 'message': 'bad ai plugin config format'}
    plugin_name = plugin_config['name']
    plugin_create_result = create_ai_plugin(app_id, plugin_name)
    if plugin_create_result['result'] == 'error':
        return plugin_create_result
    plugin_id = plugin_create_result['data']['id']
    endpoint = plugin_config.get('endpoint','')
    plugin_envs = plugin_config.get('envs', {})
    plugin_headers = plugin_config.get('headers', {})
    plugin_params = plugin_config.get('params', {})
    configure_ai_plugin(app_id, plugin_id, plugin_name, endpoint, plugin_headers, plugin_envs, plugin_params)
    for function_info in plugin_config.get('functions', []):
        try:
            function_name = function_info['name']
            description = function_info['description']
            parameters = function_info['parameters']
            function_call = function_info['function_call']
            add_ai_function_to_ai_plugin(app_id, plugin_id, function_name, description, parameters, function_call)
        except Exception as e:
            logging.info(f"fail to add_ai_function_to_ai_plugin:app_id:{app_id}, plugin_id:{plugin_id}, function_info:{function_info}")
            logging.exception(e)
            pass
    return {'result': 'ok', 'data':{'success':True}}

def plugin_publish(app_id, plugin_id, name, description, order):
    export_result = plugin_export(app_id, plugin_id)
    if export_result['result'] == 'error':
        return export_result
    plugin_config = json.loads(export_result['data']['file']['content'])
    redis = lanying_redis.get_redis_connection()
    public_id = str(redis.incrby(plugin_public_info_id_generator_key(), 1))
    info_key = plugin_public_info_key(public_id)
    redis.hmset(info_key, {
        'public_id': public_id,
        'name': name,
        'description': description,
        'order': order,
        'config': json.dumps(plugin_config, ensure_ascii=False)
    })
    list_key = plugin_publish_id_list_key()
    redis.rpush(list_key, public_id)
    return {'result':'ok', 'data':{'success':True, 'public_id':public_id}}

def list_public_plugins():
    redis = lanying_redis.get_redis_connection()
    list_key = plugin_publish_id_list_key()
    public_ids = lanying_redis.redis_lrange(redis, list_key, 0, -1)
    plugin_infos = []
    for public_id in public_ids:
        public_plugin_info = get_public_plugin(public_id)
        if public_plugin_info:
            plugin_info = {
                'public_id': public_id,
                'name': public_plugin_info['name'],
                'description': public_plugin_info['description'],
                'order': public_plugin_info['order']
            }
            plugin_infos.append(plugin_info)
    return {'result':'ok', 'data':{'list':plugin_infos}}

def get_public_plugin(public_id):
    redis = lanying_redis.get_redis_connection()
    info_key = plugin_public_info_key(public_id)
    info = lanying_redis.redis_hgetall(redis, info_key)
    if 'name' in info:
        return info
    return None

def plugin_publish_id_list_key():
    return f"lanying-connector:public-plugin:list"

def plugin_public_info_key(public_id):
    return f"lanying-connector:public-plugin:info:{public_id}"

def plugin_public_info_id_generator_key():
    return f"lanying-connector:public-plugin:info_id_generator"

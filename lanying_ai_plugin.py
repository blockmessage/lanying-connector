import logging
import time
import lanying_redis
import json
import lanying_embedding
from urllib.parse import urlparse
from urllib.parse import urlunparse
import lanying_config
import random

def create_ai_plugin(app_id, plugin_name):
    logging.info(f"start create ai plugin: app_id:{app_id}, plugin_name:{plugin_name}")
    now = int(time.time())
    embedding_name = maybe_create_function_embedding(app_id)
    embedding_name_info = lanying_embedding.get_embedding_name_info(app_id, embedding_name)
    embedding_uuid = embedding_name_info['embedding_uuid']
    doc_id = lanying_embedding.generate_doc_id(embedding_uuid)
    redis = lanying_redis.get_redis_connection()
    plugin_id = generate_ai_plugin_id()
    lanying_embedding.create_doc_info(app_id, embedding_uuid, f'dummy_filename_{plugin_id}', f'dummy_object_name_{plugin_id}', doc_id, 0, '.plugin', 'plugin', f'dummy_url_{plugin_id}', "openai",{})
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
            for key in ["name", "plugin_id", "create_time", "headers", "endpoint"]:
                if key in info:
                    if key in ["headers"]:
                        dto[key] = json.loads(info[key])
                    else:
                        dto[key] = info[key]
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
            for key in ["function_id", "plugin_id", "create_time", "name", "description", "parameters", "callback"]:
                if key in info:
                    if key in ["parameters", "callback"]:
                        dto[key] = json.loads(info[key])
                    else:
                        dto[key] = info[key]
            result.append(dto)
    return {'result':'ok', 'data':{'list': result, 'total': total}}

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

def add_ai_function_to_ai_plugin(app_id, plugin_id, name, description, parameters, callback):
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
        'callback': json.dumps(callback, ensure_ascii=False)
    }
    function_info = {
        'name': name,
        'description': description,
        'parameters': parameters,
        'callback': callback
    }
    function = json.dumps(function_info, ensure_ascii=False)
    text = description
    token_cnt = lanying_embedding.num_of_tokens(function)
    blocks = [(token_cnt, "function", text, function, block_id)]
    embedding_uuid_info = lanying_embedding.get_embedding_uuid_info(embedding_uuid)
    redis = lanying_redis.get_redis_connection()
    redis_stack = lanying_redis.get_redis_stack_connection()
    lanying_embedding.insert_embeddings(embedding_uuid_info, app_id, embedding_uuid,"function", doc_id, blocks, redis_stack)
    increase_ai_function_count(app_id, 1)
    redis.hmset(get_ai_function_key(app_id, function_id), ai_function_info)
    redis.rpush(get_ai_function_ids_key(app_id, plugin_id), function_id)
    return {'result':'ok', 'data':{'function_id': function_id}}

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

def configure_ai_plugin(app_id, plugin_id, name, headers, endpoint):
    ai_plugin_info = get_ai_plugin(app_id, plugin_id)
    if not ai_plugin_info:
        return {'result':'error', 'message': 'ai plugin not exist'}
    redis = lanying_redis.get_redis_connection()
    redis.hmset(get_ai_plugin_key(app_id, plugin_id), {
        'name': name,
        'headers': json.dumps(headers, ensure_ascii=False),
        'endpoint': endpoint
    })
    return {'result':'ok', 'data':{'success': True}}

def configure_ai_function(app_id, plugin_id, function_id, name, description, parameters,callback):
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
        'callback': json.dumps(callback, ensure_ascii=False)
    })
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

def fill_function_info(app_id, function_info, doc_id):
    if doc_id == '':
        return function_info
    plugin_id = get_plugin_id_by_doc_id(app_id, doc_id)
    if not plugin_id:
        return function_info
    plugin_info = get_ai_plugin(app_id, plugin_id)
    if not plugin_info:
        return function_info
    headers_str = plugin_info.get('headers', '{}')
    headers = json.loads(headers_str)
    callback = function_info.get('callback', {})
    callback_headers = callback.get('headers', {})
    endpoint = plugin_info.get('endpoint', '')
    if len(headers) > 0:
        for k,v in headers:
            callback_headers[k] = v
        callback['headers'] = callback_headers
    if len(endpoint) > 0:
        callback_url = callback.get('url', '')
        old_urlparse = urlparse(callback_url)
        new_urlparse = urlparse(endpoint)
        new_url = urlunparse(old_urlparse._replace(netloc=new_urlparse.netloc,scheme=new_urlparse.scheme))
        callback["url"] = new_url
    function_info["callback"] = callback

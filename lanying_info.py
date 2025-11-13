import lanying_config
import lanying_chatbot
import lanying_embedding
import json
lanying_config.init()

cache = {}

def set_cache(key, value):
    old_value = cache.get(key)
    if old_value != value:
        print(f'set env {key} from {old_value} to {value}')
    cache[key] = value

def get_cache(key):
    return cache.get(key)

def info():
    while True:
        line = input('==> ')
        if line == 'quit':
            break
        if line == '':
            continue
        if line == 'env':
            print(cache)
        fields = line.split(' ,')
        if len(fields) == 1:
            dump_results(info1(*fields))
        elif len(fields) == 2:
            dump_results(info2(*fields))
        else:
            print("unknown command")

def dump_results(list):
    for item in list:
        for key,value in item.items():
            print(f"========{key}===========")
            print(json.dumps(value, ensure_ascii=False, indent=2))
            print('')

def info1(any):
    rules = [info_chatbot_ids, info_embedding_ids, info_lanying_connector, info_embedding_uuid_info, info_embedding_doc_id_list, info_chatbot, info_embedding_name_info, info_embedding_doc_info]
    results = []
    for rule in rules:
        try:
            result = rule(any)
            if isinstance(result, dict) and result['result'] == 'ok':
                results.append(result['data'])
        except Exception as e:
            pass
    return results

def info2(any1, any2):
    rules = []
    results = []
    for rule in rules:
        try:
            result = rule(any1, any2)
            if isinstance(result, dict) and result['result'] == 'ok':
                results.append(result['data'])
        except Exception as e:
            pass
    return results

def info_chatbot_ids(app_id):
    if is_app_id(app_id):
        list = lanying_chatbot.get_chatbot_ids(app_id)
        if len(list) > 0:
            set_cache('app_id', app_id)
            results = []
            for chatbot_id in list:
                chatbot_info = lanying_chatbot.get_chatbot(app_id, chatbot_id)
                if chatbot_info:
                    results.append({
                        'chatbot_id': chatbot_id,
                        'chatbot_name': chatbot_info.get('name')
                    })
            return {
                'result': 'ok',
                'data': {
                    'chatbot_ids': results
                }
            }

def info_chatbot(chatbot_id):
    app_id = get_cache('app_id')
    if is_app_id(app_id):
        chatbot = lanying_chatbot.get_chatbot(app_id, chatbot_id)
        if chatbot:
            bind_embeddings = []
            lanying_connector = lanying_config.get_lanying_connector(app_id)
            if lanying_connector:
                preset_name = chatbot.get('name')
                bind_embeddings = lanying_embedding.get_preset_embedding_infos(lanying_connector.get('embeddings'), app_id, preset_name)
            return {
                'result': 'ok',
                'data': {
                    'chatbot': chatbot,
                    'bind_embeddings': bind_embeddings
                }
            }

def info_embedding_ids(app_id):
    if is_app_id(app_id):
        list = lanying_embedding.list_embedding_names(app_id)
        if len(list) > 0:
            set_cache('app_id', app_id)
            results = []
            for embedding_name in list:
                embedding_name_info = lanying_embedding.get_embedding_name_info(app_id, embedding_name)
                if embedding_name_info:
                    embedding_uuid = embedding_name_info['embedding_uuid']
                    results.append({
                        'embedding_name': embedding_name,
                        'embedding_uuid': embedding_uuid,
                    })
            if len(results) > 0:
                return {
                    'result': 'ok',
                    'data': {
                        'embedding_infos': results
                    }
                }

def info_embedding_name_info(embedding_name):
    app_id = get_cache('app_id')
    if is_app_id(app_id):
        embedding_name_info = lanying_embedding.get_embedding_name_info(app_id, embedding_name)
        if embedding_name_info:
            embedding_uuid = embedding_name_info['embedding_uuid']
            embedding_uuid_info = lanying_embedding.get_embedding_uuid_info(embedding_uuid)
            set_cache('embedding_name', embedding_name)
            set_cache('embedding_uuid', embedding_uuid)
            return {
                'result': 'ok',
                'data': {
                    'embedding_name_info': embedding_name_info,
                    'embedding_uuid_info': embedding_uuid_info
                }
            }

def info_embedding_uuid_info(embedding_uuid):
    embedding_uuid_info = lanying_embedding.get_embedding_uuid_info(embedding_uuid)
    if embedding_uuid_info:
        embedding_name = embedding_uuid_info['embedding_name']
        app_id = embedding_uuid_info['app_id']
        embedding_name_info = lanying_embedding.get_embedding_name_info(app_id, embedding_name)
        set_cache('embedding_name', embedding_name)
        set_cache('embedding_uuid', embedding_uuid)
        return {
            'result': 'ok',
            'data': {
                'embedding_name_info': embedding_name_info,
                'embedding_uuid_info': embedding_uuid_info
            }
        }

def info_embedding_doc_id_list(embedding_uuid):
    embedding_doc_id_list = lanying_embedding.get_embedding_doc_id_list(embedding_uuid, 0, -1)
    if len(embedding_doc_id_list) > 0:
        return {
            'result': 'ok',
            'data': {
                'embedding_doc_id_list': embedding_doc_id_list
            }
        }

def info_embedding_doc_info(doc_id):
    embedding_uuid = get_cache('embedding_uuid')
    doc_info = lanying_embedding.get_doc(embedding_uuid, doc_id)
    if doc_info:
        return {
            'result': 'ok',
            'data': {
                'doc_info': doc_info
            }
        }

def info_lanying_connector(app_id):
    lanying_connector = lanying_config.get_lanying_connector(app_id)
    if lanying_connector:
        set_cache('app_id', app_id)
        return {
            'result': 'ok',
            'data': {
                'lanying_connector': lanying_connector
            }
        }

def is_app_id(any):
    if isinstance(any, str) and len(any) > 0 and len(any) < 20:
        return True
    return False

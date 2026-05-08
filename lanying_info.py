import lanying_config
import lanying_chatbot
import lanying_embedding
import json
import lanying_pgvector
import lanying_grow_ai
import lanying_openclaw
lanying_config.init()

cache = {}

def set_cache(key, value):
    old_value = cache.get(key)
    if old_value != value:
        print(f'set env {key} from {old_value} to {value}')
    cache[key] = value

def get_cache(key):
    return cache[key]

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
    rules = [info_chatbot_ids,
             info_embedding_ids,
             info_lanying_connector,
             info_openclaw_app_manager_user,
             info_openclaw_nodes,
             info_openclaw_node,
             info_openclaw_session_mapping_list,
             info_openclaw_session_mapping_detail_list,
             info_openclaw_session_mapping,
             info_embedding_uuid_info,
             info_embedding_doc_id_list_by_embedding_name,
             info_embedding_doc_id_list,
             info_chatbot,
             info_embedding_name_info,
             info_embedding_doc_info,
             info_embedding_doc_block_ids,
             info_embedding_doc_block_info,
             info_grow_ai_task_id_list,
             info_grow_ai_task_info,
             info_grow_ai_task_run_id_list,
             info_grow_ai_task_run_id_info
             ]
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
def info_embedding_doc_id_list_by_embedding_name(embedding_name):
    app_id = get_cache('app_id')
    if is_app_id(app_id):
        embedding_name_info = lanying_embedding.get_embedding_name_info(app_id, embedding_name)
        if embedding_name_info:
            embedding_uuid = embedding_name_info['embedding_uuid']
            return info_embedding_doc_id_list(embedding_uuid)

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
        set_cache('doc_id', doc_id)
        return {
            'result': 'ok',
            'data': {
                'doc_info': doc_info
            }
        }

def info_embedding_doc_block_ids(doc_id):
    embedding_uuid = get_cache('embedding_uuid')
    doc_info = lanying_embedding.get_doc(embedding_uuid, doc_id)
    if doc_info:
        embedding_uuid_info = lanying_embedding.get_embedding_uuid_info(embedding_uuid)
        db_table_name = embedding_uuid_info['db_table_name']
        count = 200
        with lanying_pgvector.get_connection() as conn:
            cursor = conn.cursor()
            query = f"SELECT block_id FROM {db_table_name} where doc_id = %s ORDER BY block_id LIMIT %s;"
            args =  [doc_id, count]
            cursor.execute(query, args)
            rows = cursor.fetchall()
            cursor.close()
            lanying_pgvector.put_connection(conn)
            ret = []
            for row in rows:
                ret.append(row[0])
            return {
                'result': 'ok',
                'data': {
                    'embedding_doc_block_ids': ret
                }
            }

def info_embedding_doc_block_info(block_id):
    doc_id = get_cache('doc_id')
    if block_id.startswith(doc_id):
        embedding_uuid = get_cache('embedding_uuid')
        embedding_uuid_info = lanying_embedding.get_embedding_uuid_info(embedding_uuid)
        db_table_name = embedding_uuid_info['db_table_name']
        count = 10
        with lanying_pgvector.get_connection() as conn:
            cursor = conn.cursor()
            query = f"SELECT id,content,doc_id,num_of_tokens,summary,text_hash,question,function,reference,block_id,tags FROM {db_table_name} where doc_id = %s and block_id = %s LIMIT %s;"
            args =  [doc_id, block_id, count]
            cursor.execute(query, args)
            rows = cursor.fetchall()
            cursor.close()
            lanying_pgvector.put_connection(conn)
            names = ['id','text','doc_id','num_of_tokens','summary','text_hash','question','function','reference','block_id', 'tags']
            ret = []
            for row in rows:
                data = {}
                for index,name in enumerate(names):
                    data[name] = row[index]
                ret.append(data)
            return {
                'result': 'ok',
                'data': {
                    'embedding_doc_block_id_info': ret
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

def info_openclaw_app_manager_user(app_id):
    if is_app_id(app_id):
        app_manager_user = lanying_openclaw.get_openclaw_app_manager_user(app_id)
        if app_manager_user is not None:
            set_cache('app_id', app_id)
            return {
                'result': 'ok',
                'data': {
                    'openclaw_app_manager_user': app_manager_user
                }
            }

def info_openclaw_nodes(app_id):
    if is_app_id(app_id):
        node_list_result = lanying_openclaw.get_node_list(app_id)
        if node_list_result.get('result') == 'ok':
            node_list = node_list_result.get('data', {}).get('list', [])
            if len(node_list) > 0:
                set_cache('app_id', app_id)
                set_cache('node_id', str(node_list[0].get('node_id', '')))
                summary = []
                for node in node_list:
                    summary.append({
                        'node_id': str(node.get('node_id', '')),
                        'name': node.get('name', ''),
                        'user_id': str(node.get('user_id', '')),
                        'chatbot_id': str(node.get('chatbot_id', '')),
                        'session_map_sync': str(node.get('session_map_sync', 'off')),
                        'merge_sub_sessions': str(node.get('merge_sub_sessions', 'off'))
                    })
                return {
                    'result': 'ok',
                    'data': {
                        'openclaw_nodes': summary
                    }
                }

def info_openclaw_node(node_id):
    app_id = cache.get('app_id')
    if is_app_id(app_id) and is_node_id(node_id):
        node_info = lanying_openclaw.get_node(app_id, str(node_id))
        if node_info is not None:
            set_cache('node_id', str(node_id))
            return {
                'result': 'ok',
                'data': {
                    'openclaw_node': node_info
                }
            }

def info_openclaw_session_mapping_list(node_id):
    app_id = cache.get('app_id')
    if is_app_id(app_id) and is_node_id(node_id):
        node_id_text = str(node_id)
        mappings = lanying_openclaw.list_session_mappings_for_node(app_id, node_id_text)
        if len(mappings) > 0:
            set_cache('node_id', node_id_text)
            summary = []
            for mapping in sorted(mappings, key=lambda item: str(item.get('session_key', ''))):
                summary.append({
                    'session_key': mapping.get('session_key', ''),
                    'group_id': str(mapping.get('group_id', '')),
                    'openclaw_user_id': str(mapping.get('openclaw_user_id', '')),
                    'management_user_id': str(mapping.get('management_user_id', '')),
                    'sender_user_id': str(mapping.get('sender_user_id', '')),
                    'parent_session_key': mapping.get('parent_session_key', ''),
                    'root_session_key': mapping.get('root_session_key', ''),
                    'effective_target_session_key': mapping.get('effective_target_session_key', ''),
                    'updated_at': int(mapping.get('updated_at', 0) or 0),
                })
            return {
                'result': 'ok',
                'data': {
                    'openclaw_session_mappings': summary
                }
            }

def info_openclaw_session_mapping_detail_list(node_id):
    app_id = cache.get('app_id')
    if is_app_id(app_id) and is_node_id(node_id):
        node_id_text = str(node_id)
        mappings = lanying_openclaw.list_session_mapping_details_for_node(app_id, node_id_text)
        if len(mappings) > 0:
            set_cache('node_id', node_id_text)
            return {
                'result': 'ok',
                'data': {
                    'openclaw_session_mapping_details': mappings
                }
            }

def info_openclaw_session_mapping(session_key):
    app_id = cache.get('app_id')
    node_id = cache.get('node_id')
    normalized_session_key = lanying_openclaw.normalize_session_key(session_key)
    if is_app_id(app_id) and is_node_id(node_id) and normalized_session_key != '':
        mapping = lanying_openclaw.get_session_mapping_by_session(app_id, str(node_id), normalized_session_key)
        if mapping is not None:
            return {
                'result': 'ok',
                'data': {
                    'openclaw_session_mapping': mapping,
                    'openclaw_session_inheritance': {
                        'session_key': mapping.get('session_key', ''),
                        'sender_user_id': str(mapping.get('sender_user_id', '')),
                        'management_user_id': str(mapping.get('management_user_id', '')),
                        'openclaw_user_id': str(mapping.get('openclaw_user_id', '')),
                        'parent_session_key': mapping.get('parent_session_key', ''),
                        'root_session_key': mapping.get('root_session_key', ''),
                        'effective_target_session_key': mapping.get('effective_target_session_key', ''),
                        'parsed_session_identity': lanying_openclaw.parse_clawchat_session_identity(mapping.get('session_key', '')),
                        'parsed_root_identity': lanying_openclaw.parse_clawchat_session_identity(mapping.get('root_session_key', ''))
                    }
                }
            }

def info_grow_ai_task_id_list(app_id):
    if is_app_id(app_id):
        id_list = lanying_grow_ai.get_task_id_list(app_id)
        if len(id_list) > 0:
            return {
                'result': 'ok',
                'data': {
                    'grow_ai_task_id_list': id_list
                }
            }

def info_grow_ai_task_info(task_id):
    app_id = get_cache('app_id')
    if is_app_id(app_id):
        task_info = lanying_grow_ai.get_task(app_id, task_id)
        if task_info:
            set_cache('grow_ai_task_id', task_id)
            return {
                'result': 'ok',
                'data': {
                    'grow_ai_task_info': task_info
                }
            }

def info_grow_ai_task_run_id_list(task_id):
    app_id = get_cache('app_id')
    if is_app_id(app_id):
        id_list = lanying_grow_ai.get_task_run_id_list(app_id, task_id)
        if len(id_list) > 0:
            set_cache('grow_ai_task_id', task_id)
            return {
                'result': 'ok',
                'data': {
                    'grow_ai_task_run_id_list': id_list
                }
            }

def info_grow_ai_task_run_id_info(task_run_id):
    app_id = get_cache('app_id')
    if is_app_id(app_id):
        info = lanying_grow_ai.get_task_run(app_id, task_run_id)
        if info:
            return {
                'result': 'ok',
                'data': {
                    'grow_ai_task_run_info': info
                }
            }

def is_app_id(any):
    if isinstance(any, str) and len(any) > 0 and len(any) < 20:
        return True
    return False

def is_node_id(any):
    return isinstance(any, str) and any.isdigit() and len(any) > 0

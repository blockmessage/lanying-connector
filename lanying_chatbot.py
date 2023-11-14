import logging
import lanying_redis
import time
import json

def create_chatbot(app_id, name, desc,  user_id, lanying_link, preset, history_msg_count_max, history_msg_count_min, history_msg_size_max, message_per_month_per_user, chatbot_ids):
    logging.info(f"start create chatbot: app_id={app_id}, name={name}, user_id={user_id}, lanying_link={lanying_link}, preset={preset}")
    now = int(time.time())
    if get_user_chatbot_id(app_id, user_id):
        return {'result':'error', 'message': 'user id already bind another chatbot'}
    if get_name_chatbot_id(app_id, name):
        return {'result':'error', 'message': 'name already exist'}
    chatbot_id = generate_chatbot_id()
    redis = lanying_redis.get_redis_connection()
    redis.hmset(get_chatbot_key(app_id, chatbot_id), {
        "chatbot_id": chatbot_id,
        "create_time": now,
        "app_id": app_id,
        "name": name,
        "desc": desc,
        "user_id": user_id,
        "lanying_link": lanying_link,
        "preset": json.dumps(preset, ensure_ascii=False),
        "history_msg_count_max": history_msg_count_max,
        "history_msg_count_min": history_msg_count_min,
        "history_msg_size_max": history_msg_size_max,
        "message_per_month_per_user": message_per_month_per_user,
        "chatbot_ids": json.dumps(chatbot_ids, ensure_ascii=False)
    })
    redis.rpush(get_chatbot_ids_key(app_id), chatbot_id)
    set_user_chatbot_id(app_id, user_id, chatbot_id)
    set_name_chatbot_id(app_id, name, chatbot_id)
    return {'result':'ok', 'data':{'id':chatbot_id}}

def configure_chatbot(app_id, chatbot_id, name, desc, user_id, lanying_link, preset, history_msg_count_max, history_msg_count_min, history_msg_size_max, message_per_month_per_user, chatbot_ids):
    logging.info(f"start configure chatbot: app_id={app_id}, chatbot_id={chatbot_id}, name={name}, user_id={user_id}, lanying_link={lanying_link}, preset={preset}")
    chatbot_info = get_chatbot(app_id, chatbot_id)
    if not chatbot_info:
        return {'result':'error', 'message': 'chatbot not exist'}
    old_user_id = chatbot_info.get('user_id')
    old_name = chatbot_info.get('name')
    if old_user_id != str(user_id):
        if get_user_chatbot_id(app_id, user_id):
            return {'result':'error', 'message': 'user id already bind another chatbot'}
    if old_name != name:
        if get_name_chatbot_id(app_id, name):
            return {'result':'error', 'message': 'name already exist'}
    redis = lanying_redis.get_redis_connection()
    redis.hmset(get_chatbot_key(app_id, chatbot_id), {
        "name": name,
        "desc": desc,
        "user_id": user_id,
        "lanying_link": lanying_link,
        "preset": json.dumps(preset, ensure_ascii=False),
        "history_msg_count_max": history_msg_count_max,
        "history_msg_count_min": history_msg_count_min,
        "history_msg_size_max": history_msg_size_max,
        "message_per_month_per_user": message_per_month_per_user,
        "chatbot_ids": json.dumps(chatbot_ids, ensure_ascii=False)
    })
    if old_user_id != str(user_id):
        if old_user_id:
            del_user_chatbot_id(app_id, old_user_id)
        set_user_chatbot_id(app_id, user_id, chatbot_id)
    if old_name != name:
        del_name_chatbot_id(app_id, old_name)
        set_name_chatbot_id(app_id, name, chatbot_id)
    return {'result':'ok', 'data':{'success': True}}

def get_default_user_id(app_id):
    chatbot_ids = get_chatbot_ids(app_id)
    if len(chatbot_ids) > 0:
        chatbot = get_chatbot(app_id, chatbot_ids[0])
        return chatbot.get('user_id')
    return None

def get_chatbot_names(app_id):
    chatbot_ids = get_chatbot_ids(app_id)
    result = []
    for chatbot_id in chatbot_ids:
        info = get_chatbot(app_id, chatbot_id)
        if info:
            result.append(info.get('name'))
    return result

def get_chatbot_by_name(app_id, chatbot_ids, name):
    chatbot_id = get_name_chatbot_id(app_id, name)
    if chatbot_id in chatbot_ids:
        chatbot = get_chatbot(app_id, chatbot_id)
        return chatbot
    return None

def get_user_chatbot_id(app_id, user_id):
    redis = lanying_redis.get_redis_connection()
    return lanying_redis.redis_hget(redis, user_chatbot_id_key(app_id), user_id)

def set_user_chatbot_id(app_id, user_id, chatbot_id):
    redis = lanying_redis.get_redis_connection()
    redis.hset(user_chatbot_id_key(app_id), user_id, chatbot_id)

def del_user_chatbot_id(app_id, user_id):
    redis = lanying_redis.get_redis_connection()
    redis.hdel(user_chatbot_id_key(app_id), user_id)

def user_chatbot_id_key(app_id):
    return f"lanying-connector:user_chatbot_id:{app_id}"

def get_name_chatbot_id(app_id, name):
    redis = lanying_redis.get_redis_connection()
    return lanying_redis.redis_hget(redis, name_chatbot_id_key(app_id), name)

def set_name_chatbot_id(app_id, name, chatbot_id):
    redis = lanying_redis.get_redis_connection()
    redis.hset(name_chatbot_id_key(app_id), name, chatbot_id)

def del_name_chatbot_id(app_id, name):
    redis = lanying_redis.get_redis_connection()
    redis.hdel(name_chatbot_id_key(app_id), name)

def name_chatbot_id_key(app_id):
    return f"lanying-connector:name_chatbot_id:{app_id}"

def is_chatbot_mode(app_id):
    redis = lanying_redis.get_redis_connection()
    return redis.incrby(chatbot_mode_key(app_id), 0) == 1

def set_chatbot_mode(app_id, mode):
    redis = lanying_redis.get_redis_connection()
    if mode:
        redis.set(chatbot_mode_key(app_id), 1)
    else:
        redis.set(chatbot_mode_key(app_id), 0)
    return True

def chatbot_mode_key(app_id):
    return f"lanying-connector:chatbot-mode:{app_id}"

def list_chatbots(app_id):
    chatbot_ids = get_chatbot_ids(app_id)
    result = []
    for chatbot_id in chatbot_ids:
        info = get_chatbot(app_id, chatbot_id)
        if info:
            dto = {}
            for key,value in info.items():
                if key in ["create_time", "user_id", "history_msg_count_max", "history_msg_count_min","history_msg_size_max","message_per_month_per_user"]:
                    dto[key] = int(value)
                elif key in ["preset"]:
                    dto[key] = json.loads(value)
                else:
                    dto[key] = value
            result.append(dto)
    return {'result':'ok', 'data':{'list': result}}

def get_chatbot_dto(app_id, chatbot_id):
    chatbot = get_chatbot(app_id, chatbot_id)
    if chatbot:
        dto = {}
        for key,value in chatbot.items():
            if key in ["create_time", "user_id", "history_msg_count_max", "history_msg_count_min","history_msg_size_max","message_per_month_per_user"]:
                dto[key] = int(value)
            elif key in ["preset"]:
                dto[key] = json.loads(value)
            else:
                dto[key] = value
        return {'result':'ok', 'data':dto}
    else:
        return {'result':'error', 'message': 'chatbot not exist'}

def get_chatbot_ids(app_id):
    redis = lanying_redis.get_redis_connection()
    list_key = get_chatbot_ids_key(app_id)
    return lanying_redis.redis_lrange(redis, list_key, 0, -1)

# def bind_embedding(app_id, type, name, value_list):
#     if type == 'embedding_list':
#         chatbot_id = name
#         chatbot_info = get_chatbot(app_id, chatbot_id)
#         if chatbot_info is None:
#             return {'result':'error', 'message': 'chatbot not exist'}
#         relation = get_embedding_bind_relation(app_id)
#         embedding_uuid_list = []
#         for embedding_uuid in value_list:
#             embedding_uuid_info = lanying_embedding.get_app_embedding_uuid_info(app_id, embedding_uuid)
#             if embedding_uuid_info:
#                 embedding_uuid_list.append(embedding_uuid)
#         relation[name] = embedding_uuid_list
#         set_embedding_bind_relation(app_id, relation)
#         return {'result':'ok', 'data':{}}
#     elif type == "chatbot_list":
#         embedding_uuid = name
#         embedding_uuid_info = lanying_embedding.get_app_embedding_uuid_info(app_id, embedding_uuid)
#         if embedding_uuid_info is None:
#             return {'result':'error', 'message':'embedding not exist'}
#         relation = get_embedding_bind_relation(app_id)
#         chatbot_ids = get_chatbot_ids(app_id)
#         for chatbot_id in chatbot_ids:
#             if chatbot_id in value_list:
#                 if chatbot_id in relation:
#                     if embedding_uuid not in relation[chatbot_id]:
#                         relation[chatbot_id].append(embedding_uuid)
#                 else:
#                     relation[chatbot_id] = [embedding_uuid]
#             else:
#                 if chatbot_id in relation:
#                     if embedding_uuid in relation[chatbot_id]:
#                         relation[chatbot_id].remove(embedding_uuid)
#         set_embedding_bind_relation(app_id, relation)
#         return {'result':'ok', 'data':{}}
#     else:
#         return {'result':'error', 'message':'bad argument: type'}

# def get_embedding_bind_relation(app_id):
#     key = embedding_bind_relation_key(app_id)
#     redis = lanying_redis.get_redis_connection()
#     str = lanying_redis.redis_get(redis, key)
#     if str:
#         return json.loads(str)
#     return {}

# def set_embedding_bind_relation(app_id, relation):
#     key = embedding_bind_relation_key(app_id)
#     redis = lanying_redis.get_redis_connection()
#     redis.set(key, json.dumps(relation, ensure_ascii=False))

# def embedding_bind_relation_key(app_id):
#     return f"lanying_connector:embedding_bind_relation:{app_id}"

def get_chatbot(app_id, chatbot_id):
    redis = lanying_redis.get_redis_connection()
    key = get_chatbot_key(app_id, chatbot_id)
    info = lanying_redis.redis_hgetall(redis, key)
    if "create_time" in info:
        return info
    return None

def generate_chatbot_id():
    redis = lanying_redis.get_redis_connection()
    return str(redis.incrby("lanying_connector:chatbot_id_generator", 1))

def get_chatbot_key(app_id, chatbot_id):
    return f"lanying_connector:chatbot:{app_id}:{chatbot_id}"

def get_chatbot_ids_key(app_id):
    return f"lanying_connector:chatbot_ids:{app_id}"
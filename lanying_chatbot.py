import logging
import lanying_redis
import time
import json
import lanying_ai_capsule
from datetime import datetime
import lanying_im_api
import lanying_utils

def create_chatbot(app_id, name, nickname, desc,  avatar, user_id, lanying_link, preset, history_msg_count_max, history_msg_count_min, history_msg_size_max, message_per_month_per_user, chatbot_ids, welcome_message, quota_exceed_reply_type, quota_exceed_reply_msg):
    logging.info(f"start create chatbot: app_id={app_id}, name={name}, user_id={user_id}, lanying_link={lanying_link}, preset={preset}")
    now = int(time.time())
    if get_user_chatbot_id(app_id, user_id):
        return {'result':'error', 'message': 'user id already bind another chatbot'}
    if get_name_chatbot_id(app_id, name):
        return {'result':'error', 'message': 'name already exist'}
    chatbot_id = generate_chatbot_id()
    capsule_id = lanying_ai_capsule.generate_capsule_id(app_id, chatbot_id)
    if nickname == '':
        nickname = name
    redis = lanying_redis.get_redis_connection()
    redis.hmset(get_chatbot_key(app_id, chatbot_id), {
        "chatbot_id": chatbot_id,
        "create_time": now,
        "app_id": app_id,
        "name": name,
        "nickname": nickname,
        "desc": desc,
        "avatar": avatar,
        "user_id": user_id,
        "lanying_link": lanying_link,
        "preset": json.dumps(preset, ensure_ascii=False),
        "history_msg_count_max": history_msg_count_max,
        "history_msg_count_min": history_msg_count_min,
        "history_msg_size_max": history_msg_size_max,
        "message_per_month_per_user": message_per_month_per_user,
        "chatbot_ids": json.dumps(chatbot_ids, ensure_ascii=False),
        "capsule_id": capsule_id,
        'welcome_message': welcome_message,
        "quota_exceed_reply_type": quota_exceed_reply_type,
        "quota_exceed_reply_msg": quota_exceed_reply_msg
    })
    redis.rpush(get_chatbot_ids_key(app_id), chatbot_id)
    set_user_chatbot_id(app_id, user_id, chatbot_id)
    set_name_chatbot_id(app_id, name, chatbot_id)
    try:
        private_info = ''
        if len(welcome_message) > 0:
            private_info = json.dumps({"welcome_message":welcome_message},ensure_ascii=False)
        lanying_im_api.set_user_profile(app_id, user_id, desc, nickname, private_info)
    except Exception as e:
        logging.exception(e)
    try:
        lanying_im_api.set_user_avatar(app_id, user_id, avatar)
    except Exception as e:
        logging.exception(e)
    return {'result':'ok', 'data':{'id':chatbot_id}}

def check_capsule_price(cycle_type, price, capsule):
    if cycle_type not in ["month", "year"]:
        return {'result': 'error', 'message': 'cycle_type must be month or year'}
    if cycle_type == 'month' and price != capsule['month_price']:
        return {'result': 'error', 'message': 'month_price changed'}
    if cycle_type == 'year' and price != capsule['year_price']:
        return {'result': 'error', 'message': 'year_price changed'}
    return {'result': 'ok'}

def check_create_chatbot_from_capsule(app_id, capsule_id, password, cycle_type, price):
    capsule = lanying_ai_capsule.get_capsule(capsule_id)
    if capsule is None:
        return {'result': 'error', 'message': 'capsule not exist'}
    if capsule['status'] != 'normal':
        return {'result': 'error', 'message': 'capsule status not normal'}
    if capsule['password'] != '' and capsule['password'] != password:
        return {'result': 'error', 'message': 'capsule password not right'}
    check_res = check_capsule_price(cycle_type, price, capsule)
    if check_res['result'] == 'error':
        return check_res
    capsule_app_id = capsule['app_id']
    capsule_chatbot_id = capsule['chatbot_id']
    capsule_chatbot = get_chatbot(capsule_app_id, capsule_chatbot_id)
    if capsule_chatbot is None:
        return {'result': 'error', 'message': 'capsule chatbot not exist'}
    return {'result': 'ok', 'data': {'success': True}}

def create_chatbot_from_capsule(app_id, capsule_id, password, cycle_type, price, user_id, lanying_link):
    capsule = lanying_ai_capsule.get_capsule(capsule_id)
    if capsule is None:
        return {'result': 'error', 'message': 'capsule not exist'}
    if capsule['status'] != 'normal':
        return {'result': 'error', 'message': 'capsule status not normal'}
    if capsule['password'] != '' and capsule['password'] != password:
        return {'result': 'error', 'message': 'capsule password not right'}
    check_res = check_capsule_price(cycle_type, price, capsule)
    if check_res['result'] == 'error':
        return check_res
    capsule_app_id = capsule['app_id']
    capsule_chatbot_id = capsule['chatbot_id']
    capsule_chatbot = get_chatbot_with_profile(capsule_app_id, capsule_chatbot_id)
    if capsule_chatbot is None:
        return {'result': 'error', 'message': 'capsule chatbot not exist'}
    name = capsule_chatbot['name']
    nickname = capsule_chatbot.get('nickname',name)
    if get_name_chatbot_id(app_id, name):
        timestr = datetime.now().strftime('%Y%m%d%H%M%S')
        name = f"{name}_{timestr}"
    avatar = capsule_chatbot.get('avatar','')
    welcome_message = capsule_chatbot.get('welcome_message','')
    quota_exceed_reply_type = capsule_chatbot.get('quota_exceed_reply_type', 'capsule')
    quota_exceed_reply_msg = capsule_chatbot.get('quota_exceed_reply_msg', '')
    create_result = create_chatbot(app_id, name, nickname, capsule_chatbot['desc'], avatar, user_id, lanying_link, capsule_chatbot['preset'], capsule_chatbot['history_msg_count_max'], capsule_chatbot['history_msg_count_min'], capsule_chatbot['history_msg_size_max'], capsule_chatbot['message_per_month_per_user'], [],welcome_message, quota_exceed_reply_type, quota_exceed_reply_msg)
    if create_result['result'] != 'ok':
        return create_result
    new_chatbot_id = create_result['data']['id']
    set_chatbot_field(app_id, new_chatbot_id, "linked_capsule_id", capsule_id)
    set_chatbot_field(app_id, new_chatbot_id, "cycle_type", cycle_type)
    set_chatbot_field(app_id, new_chatbot_id, "price", price)
    lanying_ai_capsule.add_capsule_app_id(capsule_id, app_id, new_chatbot_id)
    return create_result

def check_create_chatbot_from_publish_capsule(app_id, capsule_id, cycle_type, price):
    capsule = lanying_ai_capsule.get_publish_capsule(capsule_id)
    if capsule is None:
        return {'result': 'error', 'message': 'capsule not exist'}
    origin_capsule = lanying_ai_capsule.get_capsule(capsule_id)
    if origin_capsule is None:
        return {'result': 'error', 'message': 'capsule not exist'}
    check_res = check_capsule_price(cycle_type, price, origin_capsule)
    if check_res['result'] == 'error':
        return check_res
    capsule_app_id = capsule['app_id']
    capsule_chatbot_id = capsule['chatbot_id']
    capsule_chatbot = get_chatbot(capsule_app_id, capsule_chatbot_id)
    if capsule_chatbot is None:
        return {'result': 'error', 'message': 'capsule chatbot not exist'}
    return {'result': 'ok', 'data': {'success': True}}

def create_chatbot_from_publish_capsule(app_id, capsule_id, cycle_type, price, user_id, lanying_link):
    capsule = lanying_ai_capsule.get_publish_capsule(capsule_id)
    if capsule is None:
        return {'result': 'error', 'message': 'capsule not exist'}
    origin_capsule = lanying_ai_capsule.get_capsule(capsule_id)
    if origin_capsule is None:
        return {'result': 'error', 'message': 'capsule not exist'}
    check_res = check_capsule_price(cycle_type, price, origin_capsule)
    if check_res['result'] == 'error':
        return check_res
    capsule_app_id = capsule['app_id']
    capsule_chatbot_id = capsule['chatbot_id']
    capsule_chatbot = get_chatbot_with_profile(capsule_app_id, capsule_chatbot_id)
    if capsule_chatbot is None:
        return {'result': 'error', 'message': 'capsule chatbot not exist'}
    name = capsule_chatbot['name']
    nickname = capsule_chatbot.get('nickname', '')
    if nickname == '':
        nickname = capsule.get('name', '')
    if nickname == '':
        nickname = name
    desc = capsule_chatbot.get('desc', '')
    if desc == '':
        desc = capsule.get('desc', '')
    if get_name_chatbot_id(app_id, name):
        timestr = datetime.now().strftime('%Y%m%d%H%M%S')
        name = f"{name}_{timestr}"
    avatar = capsule_chatbot.get('avatar','')
    welcome_message = capsule_chatbot.get('welcome_message','')
    quota_exceed_reply_type = capsule_chatbot.get('quota_exceed_reply_type', 'capsule')
    quota_exceed_reply_msg = capsule_chatbot.get('quota_exceed_reply_msg', '')
    create_result = create_chatbot(app_id, name, nickname, capsule_chatbot['desc'], avatar, user_id, lanying_link, capsule_chatbot['preset'], capsule_chatbot['history_msg_count_max'], capsule_chatbot['history_msg_count_min'], capsule_chatbot['history_msg_size_max'], capsule_chatbot['message_per_month_per_user'], [],welcome_message, quota_exceed_reply_type, quota_exceed_reply_msg)
    if create_result['result'] != 'ok':
        return create_result
    new_chatbot_id = create_result['data']['id']
    set_chatbot_field(app_id, new_chatbot_id, "linked_publish_capsule_id", capsule_id)
    set_chatbot_field(app_id, new_chatbot_id, "cycle_type", cycle_type)
    set_chatbot_field(app_id, new_chatbot_id, "price", price)
    lanying_ai_capsule.add_capsule_app_id(capsule_id, app_id, new_chatbot_id)
    return create_result

def set_chatbot_field(app_id, chatbot_id, field, value):
    redis = lanying_redis.get_redis_connection()
    redis.hset(get_chatbot_key(app_id, chatbot_id), field, value)

def delete_chatbot(app_id, chatbot_id):
    chatbot_info = get_chatbot(app_id, chatbot_id)
    if chatbot_info is None:
        return {'result': 'error', 'message': 'chatbot not exist'}
    redis = lanying_redis.get_redis_connection()
    user_id = chatbot_info.get('user_id')
    name = chatbot_info.get('name')
    del_user_chatbot_id(app_id, user_id)
    del_name_chatbot_id(app_id, name)
    redis.lrem(get_chatbot_ids_key(app_id), 1, chatbot_id)
    return {'result':'ok', 'data':{}}

def delete_chatbots(app_id):
    chatbots = list_chatbots(app_id)
    for chatbot in chatbots:
        chatbot_id = chatbot.get('chatbot_id')
        delete_chatbot(app_id, chatbot_id)
    return {'result':'ok', 'data':{}}

def configure_chatbot(app_id, chatbot_id, name,nickname, desc, avatar, user_id, lanying_link, preset, history_msg_count_max, history_msg_count_min, history_msg_size_max, message_per_month_per_user, chatbot_ids, welcome_message, quota_exceed_reply_type, quota_exceed_reply_msg):
    logging.info(f"start configure chatbot: app_id={app_id}, chatbot_id={chatbot_id}, name={name}, user_id={user_id}, lanying_link={lanying_link}, preset={preset}, quota_exceed_reply_type={quota_exceed_reply_type}, quota_exceed_reply_msg={quota_exceed_reply_msg}")
    chatbot_info = get_chatbot(app_id, chatbot_id)
    if not chatbot_info:
        return {'result':'error', 'message': 'chatbot not exist'}
    old_user_id = chatbot_info.get('user_id')
    old_name = chatbot_info.get('name')
    if old_user_id != user_id:
        if get_user_chatbot_id(app_id, user_id):
            return {'result':'error', 'message': 'user id already bind another chatbot'}
    if old_name != name:
        if get_name_chatbot_id(app_id, name):
            return {'result':'error', 'message': 'name already exist'}
    redis = lanying_redis.get_redis_connection()
    redis.hmset(get_chatbot_key(app_id, chatbot_id), {
        "name": name,
        "desc": desc,
        "nickname": nickname,
        "avatar": avatar,
        "user_id": user_id,
        "lanying_link": lanying_link,
        "preset": json.dumps(preset, ensure_ascii=False),
        "history_msg_count_max": history_msg_count_max,
        "history_msg_count_min": history_msg_count_min,
        "history_msg_size_max": history_msg_size_max,
        "message_per_month_per_user": message_per_month_per_user,
        "chatbot_ids": json.dumps(chatbot_ids, ensure_ascii=False),
        'welcome_message': welcome_message,
        "quota_exceed_reply_type": quota_exceed_reply_type,
        "quota_exceed_reply_msg": quota_exceed_reply_msg
    })
    if old_user_id != user_id:
        if old_user_id:
            del_user_chatbot_id(app_id, old_user_id)
        set_user_chatbot_id(app_id, user_id, chatbot_id)
    if old_name != name:
        del_name_chatbot_id(app_id, old_name)
        set_name_chatbot_id(app_id, name, chatbot_id)
    if desc != chatbot_info.get('desc', '') or nickname != chatbot_info.get('nickname', '') or welcome_message !=  chatbot_info.get('welcome_message', ''):
        try:
            private_info = {}
            try:
                profile = lanying_im_api.get_user_profile(app_id, user_id)
                private_info = lanying_utils.safe_json_loads(profile["data"].get('private_info', '{}'))
            except Exception as ee:
                logging.exception(ee)
            private_info['welcome_message'] = welcome_message
            private_info_str =  json.dumps(private_info, ensure_ascii=False)
            lanying_im_api.set_user_profile(app_id, user_id, desc, nickname, private_info_str)
        except Exception as e:
            logging.exception(e)
            pass
    if avatar != chatbot_info.get('avatar', ''):
        try:
            lanying_im_api.set_user_avatar(app_id, user_id, avatar)
        except Exception as e:
            logging.exception(e)
            pass
    return {'result':'ok', 'data':{'success': True}}

def delete_chatbot(app_id, chatbot_id):
    logging.info(f"start check delete chatbot: app_id={app_id}, chatbot_id={chatbot_id}")
    chatbot_info = get_chatbot(app_id, chatbot_id)
    if not chatbot_info:
        return {'result':'error', 'message': 'chatbot not exist'}
    capsule_id = chatbot_info['capsule_id']
    capsule_info = lanying_ai_capsule.get_capsule(capsule_id)
    if capsule_info:
        return {'result': 'error', 'message': 'shared chatbot cannot be deleted'}
    logging.info(f"start delete chatbot: app_id={app_id}, chatbot_id={chatbot_id}")
    chatbot_name = chatbot_info['name']
    chatbot_user_id = chatbot_info['user_id']
    redis = lanying_redis.get_redis_connection()
    redis.delete(get_chatbot_key(app_id, chatbot_id))
    redis.lrem(get_chatbot_ids_key(app_id), 0, chatbot_id)
    del_user_chatbot_id(app_id, chatbot_user_id)
    del_name_chatbot_id(app_id, chatbot_name)
    from lanying_ai_plugin import delete_chatbot_plugin_bind_relation
    delete_chatbot_plugin_bind_relation(app_id, chatbot_name)
    logging.info(f"finish delete chatbot: app_id={app_id}, chatbot_id={chatbot_id}")
    return {'result': 'ok', 'data':{
        'name': chatbot_name
    }}

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
        info = get_chatbot_with_profile(app_id, chatbot_id)
        if info:
            result.append(info)
    return result

def get_chatbot_dto(app_id, chatbot_id):
    chatbot = get_chatbot_with_profile(app_id, chatbot_id)
    if chatbot:
        return {'result':'ok', 'data':chatbot}
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
        dto = {}
        for key,value in info.items():
            if key in ["create_time", "user_id", "history_msg_count_max", "history_msg_count_min","history_msg_size_max","message_per_month_per_user", "price"]:
                dto[key] = int(value)
            elif key in ["preset","chatbot_ids"]:
                dto[key] = json.loads(value)
            else:
                dto[key] = value
        if "capsule_id" not in info:
            capsule_id = lanying_ai_capsule.generate_capsule_id(app_id, chatbot_id)
            redis.hsetnx(get_chatbot_key(app_id, chatbot_id), "capsule_id", capsule_id)
            dto["capsule_id"] = capsule_id
        if 'quota_exceed_reply_type' not in dto:
            dto['quota_exceed_reply_type'] = 'capsule'
        if 'quota_exceed_reply_msg' not in dto:
            dto['quota_exceed_reply_msg'] = ''
        if 'cycle_type' not in dto:
            dto['cycle_type'] = 'month'
        if 'price' not in dto:
            dto['price'] = 0
        return dto
    return None

def get_chatbot_with_profile(app_id, chatbot_id):
    chatbot = get_chatbot(app_id, chatbot_id)
    if chatbot:
        app_id = chatbot['app_id']
        user_id = chatbot['user_id']
        try:
            result = lanying_im_api.get_user_profile(app_id, user_id)
            if result.get('code') ==  200:
                chatbot['nickname'] = result['data'].get('nick_name', '')
                chatbot['avatar'] = result['data'].get('avatar', '')
                chatbot['desc'] = result['data'].get('description', '')
                if len(chatbot['avatar']) > 0:
                    try:
                        avatar_download_url = lanying_im_api.get_avatar_real_download_url(app_id,user_id, chatbot['avatar'])
                        chatbot['avatar_download_url'] = avatar_download_url
                    except Exception as e:
                        chatbot['avatar_download_url'] = ''
                else:
                    chatbot['avatar_download_url'] = ''
        except Exception as e:
            pass
    return chatbot

def get_chatbot_profile(app_id, user_id):
    try:
        result = lanying_im_api.get_user_profile(app_id, user_id)
        if result.get('code') ==  200:
            return {
                'nickname': result['data'].get('nick_name', ''),
                'avatar':result['data'].get('avatar', ''),
                'desc' : result['data'].get('description', ''),
                'private_info': result['data'].get('private_info', '')
            }
    except Exception as e:
        return {}

def generate_chatbot_id():
    redis = lanying_redis.get_redis_connection()
    return str(redis.incrby("lanying_connector:chatbot_id_generator", 1))

def get_chatbot_key(app_id, chatbot_id):
    return f"lanying_connector:chatbot:{app_id}:{chatbot_id}"

def get_chatbot_ids_key(app_id):
    return f"lanying_connector:chatbot_ids:{app_id}"
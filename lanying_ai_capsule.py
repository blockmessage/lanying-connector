import logging
import time
import random
import lanying_redis

def share_capsule(app_id, chatbot_id, name, desc, link, password):
    from lanying_chatbot import get_chatbot
    logging.info(f"start set capsule: app_id:{app_id}, chatbot_id:{chatbot_id}, type:{type}, name:{name}, desc:{desc}, link:{link}")
    now = int(time.time())
    chatbot = get_chatbot(app_id, chatbot_id)
    if chatbot is None:
        return {'result':'error', 'message':'chatbot not exist'}
    redis = lanying_redis.get_redis_connection()
    capsule_id = chatbot['capsule_id']
    old_capsule = get_capsule(capsule_id)
    if old_capsule:
        if old_capsule["status"] != "normal":
            return {'result':'error', 'message': 'capsule status is not normal'}
        if old_capsule["app_id"] != app_id or old_capsule["chatbot_id"] != chatbot_id:
            return {'result':'error', 'message': 'capsule app_id or chatbot_id not match'}
    redis.hmset(get_capsule_key(capsule_id), {
        "capsule_id": capsule_id,
        "app_id": app_id,
        "chatbot_id": chatbot_id,
        "create_time": now,
        "name": name,
        "desc": desc,
        "link": link,
        "password": password,
        "status": "normal"
    })
    redis.hset(get_capsule_ids(app_id), capsule_id, chatbot_id)
    return {'result':'ok', 'data':{'capsule_id':capsule_id}}

def add_capsule_app_id(capsule_id, app_id, chatbot_id):
    redis = lanying_redis.get_redis_connection()
    redis.hset(capsule_app_ids_key(capsule_id), chatbot_id, app_id)

def capsule_app_ids_key(capsule_id):
    return f"lanying_connector:capsule_app_ids:{capsule_id}"

def publish_capsule(app_id, chatbot_id, type, name, desc, order):
    from lanying_chatbot import get_chatbot
    logging.info(f"start publish capsule | app_id:{app_id}, chatbot_id:{chatbot_id}, type:{type}, name:{name}, desc:{desc}, order:{order}")
    now = int(time.time())
    chatbot = get_chatbot(app_id, chatbot_id)
    if chatbot is None:
        return {'result':'error', 'message':'chatbot not exist'}
    capsule_id = chatbot['capsule_id']
    redis = lanying_redis.get_redis_connection()
    redis.hmset(get_publish_capsule_key(capsule_id), {
        "app_id": app_id,
        "chatbot_id": chatbot_id,
        "create_time": now,
        "type": type,
        "name": name,
        "desc": desc,
        "order": order,
        "capsule_id": capsule_id
    })
    redis.zadd(get_publish_capsule_ids_key(), {
        capsule_id: order
    })
    return {'result':'ok', 'data':{'success':True}}

def get_publish_capsule(capsule_id):
    redis = lanying_redis.get_redis_connection()
    key = get_publish_capsule_key(capsule_id)
    info = lanying_redis.redis_hgetall(redis, key)
    if "create_time" in info:
        dto = {}
        for key,value in info.items():
            if key in ["create_time", "order"]:
                dto[key] = int(value)
            else:
                dto[key] = value
        return dto
    return None
    
def list_publish_capsules(page_num, page_size):
    redis = lanying_redis.get_redis_connection()
    ids_key = get_publish_capsule_ids_key()
    total = redis.zcard(ids_key)
    capsule_ids = lanying_redis.redis_zrange(redis, ids_key, page_num * page_size, page_num * page_size + page_size)
    dtos = []
    for capsule_id in capsule_ids:
        capsule = get_publish_capsule(capsule_id)
        if capsule:
            dto = {}
            for key in ["capsule_id","create_time", "type", "name", "desc"]:
                dto[key] = capsule[key]
            dtos.append(dto)
    return {'result':'ok', 'data':{'list':dtos, 'total':total}}

def get_publish_capsule_ids_key():
    return f"lanying_connector:publish_capsule_ids"

def get_publish_capsule_key(capsule_id):
    return f"lanying_connector:publish_capsule:{capsule_id}"

def list_app_capsules(app_id):
    redis = lanying_redis.get_redis_connection()
    capsule_ids = lanying_redis.redis_hkeys(redis, get_capsule_ids(app_id))
    rets = []
    for capsule_id in capsule_ids:
        capsule = get_capsule(capsule_id)
        if capsule and capsule['app_id'] == app_id:
            rets.append(capsule)
    return rets

def get_capsule(capsule_id):
    redis = lanying_redis.get_redis_connection()
    key = get_capsule_key(capsule_id)
    info = lanying_redis.redis_hgetall(redis, key)
    if "create_time" in info:
        dto = {}
        for key,value in info.items():
            if key in ["create_time"]:
                dto[key] = int(value)
            else:
                dto[key] = value
        return dto
    return None

def get_capsule_key(capsule_id):
    return f"lanying_connector:capsule:{capsule_id}"

def get_capsule_ids(app_id):
    return f"lanying_connector:capsule:{app_id}"

def generate_capsule_id(app_id, chatbot_id):
    value = f"{app_id}:{chatbot_id}"
    for i in range(20):
        try:
            redis = lanying_redis.get_redis_connection()
            capsule_id = str(random.randint(100000,1000000))
            res = redis.hsetnx(capsule_id_generator(), capsule_id, value)
            if res > 0:
                return capsule_id
        except Exception as e:
            pass
    for i in range(50):
        try:
            redis = lanying_redis.get_redis_connection()
            capsule_id = str(random.randint(100000,10000000))
            res = redis.hsetnx(capsule_id_generator(), capsule_id, value)
            if res > 0:
                return capsule_id
        except Exception as e:
            pass
    for i in range(100):
        try:
            redis = lanying_redis.get_redis_connection()
            capsule_id = str(random.randint(100000,100000000))
            res = redis.hsetnx(capsule_id_generator(), capsule_id, value)
            if res > 0:
                return capsule_id
        except Exception as e:
            pass
    raise Exception('fail to generate capsule_id')

def capsule_id_generator():
    return "lanying-connector:capsule_id_generator"

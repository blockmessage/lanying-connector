import lanying_redis
import time
import json
import os
import logging
import importlib

def run_all_schedules():
    is_enable = os.getenv("ENABLE_LANYING_SCHEDULE", "0")
    logging.info(f"run_all_schedules flag: {is_enable}")
    if is_enable != "1":
        logging.info("run_all_schedules not enabled")
        return
    if not get_schedule_lock():
        logging.info("run_all_schedules get lock failed")
        return
    logging.info("run_all_schedules get lock success")
    now = int(time.time())
    schedule_ids = get_schedule_id_list()
    for schedule_id in schedule_ids:
        try:
            schedule_info = get_schedule(schedule_id)
            if schedule_info:
                module = schedule_info['module']
                interval = schedule_info['interval']
                interval = max(interval, 60 * 10)
                last_time = schedule_info['last_time']
                time_zone_diff = 3600 * 8
                if ((now + time_zone_diff) // interval) != ((last_time + time_zone_diff) // interval):
                    update_schedule_field(schedule_id, 'last_time', now)
                    logging.info(f"run_all_schedules schedule called | {schedule_info}")
                    callback_module = importlib.import_module(module)
                    callback_module.handle_schedule(schedule_info)
        except Exception as e:
            logging.exception(e)

def get_all_schedules():
    schedule_infos = []
    schedule_ids = get_schedule_id_list()
    for schedule_id in schedule_ids:
        schedule_info = get_schedule(schedule_id)
        if schedule_info:
            schedule_infos.append(schedule_info)
    return schedule_infos

def create_schedule(interval, module, args):
    schedule_id = generate_schedule_id()
    redis = lanying_redis.get_redis_connection()
    schedule_info_key = get_schedule_info_key(schedule_id)
    now = int(time.time())
    redis.hmset(schedule_info_key, {
        'schedule_id': schedule_id,
        'interval': interval,
        'create_time': now,
        'last_time': now,
        'module': module,
        'args': json.dumps(args, ensure_ascii=False)
    })
    list_key = get_schedule_list_key()
    redis.rpush(list_key, schedule_id)
    return {
        'result': 'ok',
        'data':{
            'schedule_id': schedule_id
        }
    }


def delete_schedule(schedule_id):
    redis = lanying_redis.get_redis_connection()
    schedule_info_key = get_schedule_info_key(schedule_id)
    list_key = get_schedule_list_key()
    redis.lrem(list_key, 1, schedule_id)
    redis.delete(schedule_info_key)

def get_schedule_id_list():
    redis = lanying_redis.get_redis_connection()
    list_key = get_schedule_list_key()
    schedule_ids = lanying_redis.redis_lrange(redis, list_key, 0, -1)
    return schedule_ids

def get_schedule(schedule_id):
    redis = lanying_redis.get_redis_connection()
    schedule_info_key = get_schedule_info_key(schedule_id)
    info = lanying_redis.redis_hgetall(redis, schedule_info_key)
    if info and 'create_time' in info:
        dto = {}
        for k,v in info.items():
            if k in ["interval", "create_time", "last_time"]:
                dto[k] = int(v)
            elif k in ["args"]:
                dto[k] = json.loads(v)
            else:
                dto[k] = v
        return dto
    return None

def update_schedule_field(schedule_id, field, value):
    redis = lanying_redis.get_redis_connection()
    schedule_info_key = get_schedule_info_key(schedule_id)
    redis.hset(schedule_info_key, field, value)
    return {
        'result': 'ok',
        'data':{
            'success': True
        }
    }

def get_schedule_info_key(schedule_id):
    return f"lanying_connector:schedule_info:{schedule_id}"

def get_schedule_list_key():
    return f"lanying_connector:schedule_list"

def generate_schedule_id():
    redis = lanying_redis.get_redis_connection()
    return str(redis.incrby("lanying_connector:schedule_id", 1))

def get_schedule_lock():
    key = "lanying_connector:schedule_lock"
    redis = lanying_redis.get_redis_connection()
    success = redis.setnx(key, 1)
    if success:
        redis.expire(key, 180)
        return success
    else:
        redis.expire(key, 180)
        return False

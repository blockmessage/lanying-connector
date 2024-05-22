import lanying_redis
import logging
import time
import lanying_chatbot
from datetime import datetime
import lanying_config
import requests
import json
import lanying_utils
import lanying_file_storage
import lanying_im_api
import lanying_image
from lanying_async import executor
import re
import zipfile
import uuid
import random
from dateutil.relativedelta import relativedelta
import os
import lanying_schedule
import lanying_chatbot
import base64

class TaskSetting:
    def __init__(self, app_id, name, note, chatbot_id, prompt, keywords, word_count_min, word_count_max, image_count, article_count, cycle_type, cycle_interval, file_list, deploy):
        self.app_id = app_id
        self.name = name
        self.note = note
        self.chatbot_id = chatbot_id
        self.prompt = prompt
        self.keywords = keywords
        self.word_count_min = word_count_min
        self.word_count_max = word_count_max
        self.image_count = image_count
        self.article_count = article_count
        self.cycle_type = cycle_type
        self.cycle_interval = cycle_interval
        self.file_list = file_list
        self.deploy = deploy

    def to_hmset_fields(self):
        return {
            'app_id': self.app_id,
            'name': self.name,
            'note': self.note,
            'chatbot_id': self.chatbot_id,
            'prompt': self.prompt,
            'keywords': self.keywords,
            'word_count_min': self.word_count_min,
            'word_count_max': self.word_count_max,
            'image_count': self.image_count,
            'article_count': self.article_count,
            'cycle_type': self.cycle_type,
            'cycle_interval': self.cycle_interval,
            'file_list': json.dumps(self.file_list, ensure_ascii=False),
            'deploy': json.dumps(self.deploy, ensure_ascii=False)
        }

def handle_schedule(schedule_info):
    logging.info(f"grow_ai handle_schedule start | {schedule_info}")
    module = schedule_info['module']
    args = schedule_info['args']
    if module == 'lanying_grow_ai':
        logging.info(f"grow_ai handle_schedule run task| {schedule_info}")
        app_id = args['app_id']
        task_id = args['task_id']
        task_info = get_task(app_id, task_id)
        if task_info:
            schedule = task_info['schedule']
            if schedule == 'on':
                run_task(app_id, task_id)
            else:
                logging.info(f"not run task for no schedule: app_id:{app_id}, task_id:{task_id}")

def set_task_schedule(app_id, task_id, schedule, message='manual'):
    logging.info(f"change task schedule {schedule} | app_id:{app_id}, task_id:{task_id}, message:{message}")
    task_info = get_task(app_id, task_id)
    if task_info and schedule in ["on", "off"]:
        update_task_field(app_id, task_id, "schedule", schedule)
        update_task_field(app_id, task_id, "schedule_message", message)
    return {'result': "ok", "data": {"success": True}}

def open_service(app_id, product_id, price, article_num, storage_size):
    service_status_key = get_service_status_key(app_id)
    redis = lanying_redis.get_redis_connection()
    now_datetime = datetime.now()
    if price > 0:
        pay_start_date = now_datetime.strftime('%Y-%m-%d')
    else:
        month_start_date = datetime(now_datetime.year, now_datetime.month, 1)
        pay_start_date = month_start_date.strftime('%Y-%m-%d')
    redis.hmset(service_status_key, {
        'app_id': app_id,
        'create_time': int(time.time()),
        'status': 'normal',
        'pay_start_date': pay_start_date,
        'product_id': product_id,
        'price': price,
        'article_num': article_num,
        'storage_size': storage_size
    })
    return {
        'result': 'ok',
        'data': {
            'success': True
        }
    }

def close_service(app_id, product_id):
    service_status_key = get_service_status_key(app_id)
    redis = lanying_redis.get_redis_connection()
    article_num = 0
    storage_size = 0
    redis.hmset(service_status_key,{
        'status': 'stopped',
        'article_num': article_num,
        'storage_size': storage_size
        })
    return {
        'result': 'ok',
        'data': {
            'success': True
        }
    }

def get_service_status(app_id):
    service_status_key = get_service_status_key(app_id)
    redis = lanying_redis.get_redis_connection()
    info = lanying_redis.redis_hgetall(redis, service_status_key)
    if 'create_time' in info:
        dto = {}
        for key,value in info.items():
            if key in ['create_time', 'product_id', 'article_num', 'storage_size']:
                dto[key] = int(value)
            else:
                dto[key] = value
        return dto
    return None

def get_service_status_key(app_id):
    return f"lanying-connector:grow_ai:service_status:{app_id}"

def get_service_usage(app_id):
    article_num_key = get_service_statistic_key_list(app_id, 'article_num')[0]
    storage_size_key = get_service_statistic_key_list(app_id, 'storage_size')[0]
    redis = lanying_redis.get_redis_connection()
    article_num = redis.incrby(article_num_key, 0)
    storage_size = redis.incrby(storage_size_key, 0)
    return {
        'result': 'ok',
        'data':{
            'article_num': article_num,
            'storage_size': storage_size
        }
}

def incrby_service_usage(app_id, field, value):
    logging.info(f"incrby_service_usage | app_id:{app_id}, field:{field}, value:{value}")
    redis = lanying_redis.get_redis_connection()
    key_list = get_service_statistic_key_list(app_id, field)
    for key in key_list:
        redis.incrby(key, value)

def get_service_statistic_key_list(app_id, field):
    if field == 'storage_size':
        return [
            f'lanying-connector:grow_ai:staistic:{field}:{app_id}'
        ]
    now = datetime.now()
    service_status = get_service_status(app_id)
    month_start_date = datetime(now.year, now.month, 1)
    if service_status:
        pay_start_date = datetime.strptime(service_status['pay_start_date'], '%Y-%m-%d')
        product_id = service_status['product_id']
    else:
        pay_start_date = month_start_date
        product_id = 0
    while now >= pay_start_date:
        end_date = pay_start_date + relativedelta(months=1)
        if now >= pay_start_date and now < end_date:
            break
        else:
            pay_start_date = end_date
    pay_start_date_str = pay_start_date.strftime('%Y-%m-%d')
    now_date_str = now.strftime('%Y-%m-%d')
    return [
        f'lanying-connector:grow_ai:staistic:{field}:pay_start_date:{app_id}:{product_id}:{pay_start_date_str}',
        f'lanying-connector:grow_ai:staistic:{field}:month_start_date:{app_id}:{product_id}:{month_start_date}',
        f'lanying-connector:grow_ai:staistic:{field}:everyday:{app_id}:{product_id}:{now_date_str}'
    ]

def create_task(task_setting: TaskSetting):
    now = int(time.time())
    app_id = task_setting.app_id
    task_id = generate_task_id()
    result = handle_task_file_list(app_id, task_id, task_setting.file_list)
    if result['result'] == 'error':
        return result
    redis = lanying_redis.get_redis_connection()
    fields = task_setting.to_hmset_fields()
    fields['status'] = 'normal'
    fields['create_time'] = now
    fields['task_id'] = task_id
    fields['schedule'] = 'on'
    logging.info(f"create task start | app_id:{app_id}, task_info:{fields}")
    redis.hmset(get_task_key(app_id, task_id), fields)
    redis.rpush(get_task_list_key(app_id), task_id)
    task_info = get_task(app_id, task_id)
    logging.info(f"create task finish | app_id:{app_id}, task_info:{task_info}")
    cycle_type = task_info['cycle_type']
    cycle_interval = task_info['cycle_interval']
    if cycle_type == 'cycle':
        result = lanying_schedule.create_schedule(cycle_interval, 'lanying_grow_ai', {'app_id':app_id, 'task_id':task_id})
        schedule_id = result['data']['schedule_id']
        update_task_field(app_id, task_id, "schedule_id", schedule_id)
    if task_info['cycle_type'] == 'none':
        executor.submit(run_task, app_id, task_id)
    set_admin_token(app_id)
    return {
        'result': 'ok',
        'data': {
            'task_id': task_id
        }
    }

def configure_task(task_id, task_setting: TaskSetting):
    now = int(time.time())
    app_id = task_setting.app_id
    task_info = get_task(app_id, task_id)
    if task_info is None:
        return {'result': 'error', 'message': 'task_id not exist'}
    result = handle_task_file_list(app_id, task_id, task_setting.file_list)
    if result['result'] == 'error':
        return result
    redis = lanying_redis.get_redis_connection()
    fields = task_setting.to_hmset_fields()
    logging.info(f"configure task start | app_id:{app_id}, task_info:{fields}")
    redis.hmset(get_task_key(app_id, task_id), fields)
    set_task_schedule(app_id, task_id, "on")
    new_task_info = get_task(app_id, task_id)
    if new_task_info['prompt'] != task_info['prompt'] or new_task_info['keywords'] != task_info['keywords'] or new_task_info['file_list'] != task_info['file_list']:
        update_task_field(app_id, task_id, "article_cursor", 0)
    if new_task_info['cycle_type'] != task_info['cycle_type'] or new_task_info['cycle_interval'] != task_info['cycle_interval']:
        schedule_id = new_task_info.get('schedule_id', '')
        if new_task_info['cycle_type'] != 'cycle':
            if schedule_id != '':
                schedule_info = lanying_schedule.get_schedule(schedule_id)
                if schedule_info:
                    lanying_schedule.delete_schedule(schedule_id)
                update_task_field(app_id, task_id, 'schedule_id', '')
        else:
            if schedule_id != '':
                schedule_info = lanying_schedule.get_schedule(schedule_id)
            else:
                schedule_info = None
            if schedule_info:
                lanying_schedule.update_schedule_field(schedule_id, 'interval', new_task_info['cycle_interval'])
                lanying_schedule.update_schedule_field(schedule_id, 'last_time', now)
            else:
                result = lanying_schedule.create_schedule(new_task_info['cycle_interval'], 'lanying_grow_ai', {'app_id':app_id, 'task_id':task_id})
                schedule_id = result['data']['schedule_id']
                update_task_field(app_id, task_id, "schedule_id", schedule_id)
    set_admin_token(app_id)
    return {
        'result': 'ok',
        'data': {
            'success': True
        }
    }

def handle_task_file_list(app_id, task_id, file_list):
    if len(file_list) > 1:
        return {'result': 'error', 'message': 'file_list len must less than or equal to 1'}
    for file in file_list:
        if 'url' in file:
            url = file['url']
            file_info = get_task_file_info(app_id, task_id, url)
            if file_info is None:
                logging.info(f"handle_task_file_list found new file:{file}")
                filename = lanying_utils.get_temp_filename(app_id, ".txt")
                config = get_dummy_lanying_connector(app_id)
                extra = {}
                user_id = lanying_chatbot.get_default_user_id(app_id)
                url = file['url']
                result = lanying_im_api.download_url(config, app_id, user_id, url, filename, extra)
                if result['result'] == 'error':
                    return {'result':'error', 'message': 'fail to download url'}
                else:
                    object_name = generate_task_file_object_name(app_id, task_id)
                    result = lanying_file_storage.upload(object_name, filename)
                    if result['result'] == 'error':
                        return {'result':'error', 'message': 'fail to upload url'}
                    else:
                        file_info = {
                            'app_id': app_id,
                            'task_id': task_id,
                            'object_name': object_name
                        }
                        logging.info(f"handle_task_file_list save file info | file_info:{file_info}")
                        set_task_file_info(app_id, task_id, url, file_info)
        else:
            return {'result':'error', 'message': 'bad file_list item'}
    return {'result': 'ok'}

def get_task_file_info(app_id, task_id, url):
    redis = lanying_redis.get_redis_connection()
    key = get_task_file_info_key(app_id, task_id)
    result = lanying_redis.redis_hget(redis, key, url)
    if result:
        return json.loads(result)
    return None

def set_task_file_info(app_id, task_id, url, value):
    redis = lanying_redis.get_redis_connection()
    key = get_task_file_info_key(app_id, task_id)
    redis.hset(key, url, json.dumps(value, ensure_ascii=False))
    
def get_task_file_info_key(app_id, task_id):
    return f"lanying_connector:grow_ai:task_file:{app_id}:{task_id}"

def get_task_list(app_id):
    redis = lanying_redis.get_redis_connection()
    task_ids = reversed(lanying_redis.redis_lrange(redis, get_task_list_key(app_id), 0, -1))
    task_list = []
    for task_id in task_ids:
        task_info = get_task(app_id, task_id)
        if task_info:
            task_list.append(task_info)
    return {
        'result': 'ok',
        'data':
            {
                'list': task_list
            }
    }

def get_task(app_id, task_id):
    redis = lanying_redis.get_redis_connection()
    key = get_task_key(app_id, task_id)
    info = lanying_redis.redis_hgetall(redis, key)
    if "create_time" in info:
        dto = {}
        for key,value in info.items():
            if key in ['word_count_min', 'word_count_max', 'image_count', 'article_count', 'cycle_interval', 'create_time', 'article_cursor']:
                dto[key] = int(value)
            elif key in ['file_list', 'deploy']:
                dto[key] = json.loads(value)
            else:
                dto[key] = value
        if 'schedule' not in info:
            dto['schedule'] = 'on'
        if 'file_list' not in info:
            dto['file_list'] = []
        if 'deploy' not in info:
            dto['deploy'] = {'type': 'none'}
        return dto
    return None

def update_task_field(app_id, task_id, field, value):
    redis = lanying_redis.get_redis_connection()
    redis.hset(get_task_key(app_id, task_id), field, value)

def increase_task_field(app_id, task_id, field, value):
    redis = lanying_redis.get_redis_connection()
    return redis.hincrby(get_task_key(app_id, task_id), field, value)

def get_task_key(app_id, task_id):
    return f"lanying_connector:grow_ai:task:{app_id}:{task_id}"

def get_task_list_key(app_id):
    return f"lanying_connector:grow_ai:task_list:{app_id}"

def generate_task_id():
    redis = lanying_redis.get_redis_connection()
    return redis.incrby("lanying_connector:grow_ai:task_id_generator", 1)

def generate_task_file_object_name(app_id, task_id):
    redis = lanying_redis.get_redis_connection()
    file_id = redis.incrby("lanying_connector:grow_ai:task_file_id_generator", 1)
    return f"grow_ai/task_file/{app_id}/{task_id}/{file_id}_{int(time.time())}.txt"

def delete_task(app_id, task_id):
    logging.info(f"delete task start | app_id:{app_id}, task_id:{task_id}")
    task_info = get_task(app_id, task_id)
    if task_info is None:
        return {'result': 'error', 'message': 'task_id not exist'}
    result = get_task_run_list(app_id, task_id)
    task_run_list = result['data']['list']
    for task_run in task_run_list:
        task_run_id = task_run['task_run_id']
        delete_task_run(app_id, task_run_id)
    
    schedule_id = task_info.get('schedule_id', '')
    if schedule_id != '':
        schedule_info = lanying_schedule.get_schedule(schedule_id)
        if schedule_info:
            lanying_schedule.delete_schedule(schedule_id)
    redis = lanying_redis.get_redis_connection()
    task_key = get_task_key(app_id, task_id)
    task_list_key = get_task_list_key(app_id)
    redis.lrem(task_list_key, 1, task_id)
    redis.delete(task_key)

## TASK RUN

def run_task(app_id, task_id, countdown=0):
    logging.info(f"run task start | app_id:{app_id}, task_id:{task_id}")
    task_info = get_task(app_id, task_id)
    if task_info is None:
        return {'result': 'error', 'message': 'task_id not exist'}
    try:
        now = int(time.time())
        redis = lanying_redis.get_redis_connection()
        article_count = task_info['article_count']
        cycle_type = task_info['cycle_type']
        task_run_id = generate_task_run_id(task_id)
        user_id = generate_dummy_user_id()
        redis.hmset(get_task_run_key(app_id, task_run_id),{
            'task_run_id': task_run_id,
            'status': 'wait',
            'create_time': now,
            'task_id': task_id,
            'user_id': user_id,
            'article_count': article_count,
            'cycle_type': cycle_type
        })
        redis.rpush(get_task_run_list_key(app_id, task_id), task_run_id)
        set_admin_token(app_id)
        from lanying_tasks import grow_ai_run_task
        grow_ai_run_task.apply_async(args = [app_id, task_run_id], countdown=countdown)
        logging.info(f"run task finish | app_id:{app_id}, task_id:{task_id}, task_run_id:{task_run_id}")
        return {
            'result': 'ok',
            'data':{
                'task_run_id': task_run_id
            }
        }
    except Exception as e:
        logging.exception(e)
        return {'result': 'error', 'message': 'internal error'}

def run_cycle_task(app_id, task_id):
    logging.info(f"run_cycle_task run | app_id:{app_id}, task_id:{task_id}")

def delete_task_run(app_id, task_run_id):
    task_run = get_task_run(app_id, task_run_id)
    if task_run is None:
        return {'result': 'ok', 'data':{'success': True}}
    file_size = task_run.get('file_size', 0)
    incrby_service_usage(app_id, 'storage_size', -file_size)
    task_id = task_run['task_id']
    redis = lanying_redis.get_redis_connection()
    task_run_list_key = get_task_run_list_key(app_id, task_id)
    redis.lrem(task_run_list_key, 1, task_run_id)
    task_run_key = get_task_run_key(app_id, task_run_id)
    redis.delete(task_run_key)
    return {'result': 'ok', 'data':{'success': True}}

def do_run_task(app_id, task_run_id, has_retry_times):
    try:
        update_task_run_field(app_id, task_run_id, "status", "running")
        result = do_run_task_internal(app_id, task_run_id, has_retry_times)
        if result['result'] == 'error':
            logging.info(f"do_run_task result | {result}")
            increase_task_run_field(app_id, task_run_id, "fail_times", 1)
            update_task_run_field(app_id, task_run_id, "error_message", result['message'])
            retry = result.get('retry', True)
            if retry:
                if has_retry_times:
                    update_task_run_field(app_id, task_run_id, "status", "retry")
                else:
                    update_task_run_field(app_id, task_run_id, "status", "error")
                raise Exception(result['message'])
            else:
                update_task_run_field(app_id, task_run_id, "status", "error")
                return result
        elif result['result'] == 'ok':
            increase_task_run_field(app_id, task_run_id, "success_times", 1)
            update_task_run_field(app_id, task_run_id, "status", "success")
            update_task_run_field(app_id, task_run_id, "error_message", '')
        return result
    except Exception as e:
        increase_task_run_field(app_id, task_run_id, "fail_times", 1)
        error_msg = 'internal error'
        try:
            error_msg = str(e.args[0])[:100]
        except Exception as ee:
            pass
        update_task_run_field(app_id, task_run_id, "error_message", error_msg)
        if has_retry_times:
            update_task_run_field(app_id, task_run_id, "status", "retry")
        else:
            update_task_run_field(app_id, task_run_id, "status", "error")
        raise e

def get_article_limit(app_id):
    return lanying_config.get_app_config_int_from_redis(app_id, 'lanying_connector.grow_ai_article_number')

def find_title(app_id, task_id, task_run_id, keywords):
    article_cursor = increase_task_field(app_id, task_id, 'article_cursor', 0)
    max = len(keywords)
    while article_cursor < max:
        title = keywords[article_cursor]
        if is_article_title_used(app_id, task_id, title):
            article_cursor = increase_task_field(app_id, task_id, 'article_cursor', 1)
        else:
            set_article_title_used(app_id, task_id, title, task_run_id)
            return {
                'result': 'ok',
                'data':{
                    'title': title
                }
            }
    return {
        'result': 'error',
        'message': 'article titles are exhausted',
        'retry': False
    }

def set_article_title_used(app_id, task_id, title, task_run_id):
    redis = lanying_redis.get_redis_connection()
    key = article_title_used_key(app_id, task_id)
    redis.hset(key, title, task_run_id)

def is_article_title_used(app_id, task_id, title):
    redis = lanying_redis.get_redis_connection()
    key = article_title_used_key(app_id, task_id)
    return redis.hexists(key, title)

def article_title_used_key(app_id, task_id):
    return f'lanying_connector:grow_ai:article_title_used:{app_id}:{task_id}'

def parse_file_keywords(app_id, task_id, file_list):
    keywords = []
    for file in file_list:
        if 'url' in file:
            try:
                url = file['url']
                file_info = get_task_file_info(app_id, task_id, url)
                if file_info:
                    object_name = file_info['object_name']
                    filename = lanying_utils.get_temp_filename(app_id, ".txt")
                    result = lanying_file_storage.download(object_name, filename)
                    if result['result'] == 'ok':
                        with open(filename, 'r') as fd:
                            # 使用 len() 函数获取文件行数
                            lines = fd.readlines()
                            for line in lines:
                                if len(line) > 0 and len(line) < 1000 and not line.isspace():
                                    keywords.append(line)
            except Exception as e:
                logging.exception(e)
    logging.info(f"parse_file_keywords finish | app_id:{app_id}, task_id:{task_id}, file_list:{file_list}, keyword count:{len(keywords)}")
    return keywords

def do_run_task_internal(app_id, task_run_id, has_retry_times):
    logging.info(f"do_run_task start | app_id:{app_id}, task_run_id:{task_run_id}, has_retry_times:{has_retry_times}")
    task_run = get_task_run(app_id, task_run_id)
    if task_run is None:
        return {'result': 'error', 'message': 'task_run not exist'}
    task_id = task_run['task_id']
    task = get_task(app_id, task_id)
    if task is None:
        return {'result': 'error', 'message': 'task not exist'}
    chatbot_id = task['chatbot_id']
    article_count = task_run['article_count']
    chatbot_info = lanying_chatbot.get_chatbot(app_id, chatbot_id)
    if chatbot_info is None:
        return {'result': 'error', 'message': 'chatbot not exist'}
    chatbot_user_id = chatbot_info['user_id']
    redis = lanying_redis.get_redis_connection()
    keywords = parse_keywords(task['keywords'])
    file_keywords = parse_file_keywords(app_id, task_id, task['file_list'])
    keywords.extend(file_keywords)
    if len(keywords) == 0:
        return {'result': 'error', 'message': 'article title not exist', 'retry': False}
    cycle_type = task_run.get('cycle_type', 'none')
    if cycle_type == 'none':
        article_count = len(keywords)
        update_task_run_field(app_id, task_run_id, "article_count", article_count)
        logging.info(f"use new article_count | {article_count}")
    run_result_key = get_task_run_result_key(app_id, task_run_id)
    article_limit = get_article_limit(app_id)
    for i in range(article_count):
        logging.info(f"do_run_task_internal for article | app_id:{app_id}, task_id:{task_id}, task_run_id:{task_run_id}, i:{i}")
        article_id = f'{task_run_id}_{i+1}'
        if redis.hexists(run_result_key, article_id):
            continue
        usage = get_service_usage(app_id)
        now_article_num = usage['data']['article_num']
        if now_article_num + 1 > article_limit:
            return {'result': 'error', 'message': 'article_num not enough', 'retry': False}
        result = find_title(app_id, task_id, task_run_id, keywords)
        if result['result'] == 'error':
            if result['message'] == 'article titles are exhausted':
                if cycle_type == 'none' and i > 0:
                    break
                elif cycle_type == 'cycle':
                    set_task_schedule(app_id, task_id, "off", result['message'])
            return result
        keyword = result['data']['title']
        result = do_run_task_article(app_id, task_run, task, article_id, chatbot_user_id, keyword)
        if result['result'] == 'error':
            logging.info(f"do_run_task error | app_id:{app_id}, task_run_id:{task_run_id}, article_id:{article_id}, keyword:{keyword}, result:{result}")
            return result
        article_info = result['article_info']
        redis.hset(run_result_key, article_id, json.dumps(article_info, ensure_ascii=False))
        increase_task_run_field(app_id, task_run_id, "article_success_count", 1)
        incrby_service_usage(app_id, 'article_num', 1)
    result = make_task_run_result_zip_file(app_id, task_run_id)
    if result['result'] == 'error':
        return result
    logging.info(f"do_run_task finish | app_id:{app_id}, task_run_id:{task_run_id}")
    deploy = task['deploy']
    deploy_type = deploy.get('type', 'none')
    if deploy_type != "none":
        from lanying_tasks import grow_ai_deply_task_run
        grow_ai_deply_task_run.apply_async(args = [app_id, task_run_id], countdown=5)
    return {'result': 'ok'}

def make_task_run_result_zip_file(app_id, task_run_id):
    logging.info(f"make_task_run_result_zip_file start | app_id:{app_id}, task_run_id:{task_run_id}")
    now = int(time.time())
    task_run = get_task_run(app_id, task_run_id)
    if task_run is None:
        return {'result': 'error', 'message': 'task_run not exist'}
    task_run_result_list = get_task_run_result_list(app_id, task_run_id)['data']['list']
    if len(task_run_result_list) == 0:
        return {'result': 'error', 'message': 'file not exist'}
    try:
        zip_filename = lanying_utils.get_temp_filename(app_id, ".zip")
        with zipfile.ZipFile(zip_filename, 'w') as zipf:
            for task_run_result in task_run_result_list:
                if 'markdown_file' in task_run_result:
                    markdown_objectname = task_run_result['markdown_file']
                    markdown_filename = lanying_utils.get_temp_filename(app_id, ".md")
                    result = lanying_file_storage.download(markdown_objectname, markdown_filename)
                    if result['result'] == 'ok':
                        zipf.write(markdown_filename, arcname=markdown_objectname)
                if 'image_file' in task_run_result:
                    image_objectname = task_run_result['image_file']
                    image_filename = lanying_utils.get_temp_filename(app_id, ".md")
                    result = lanying_file_storage.download(image_objectname, image_filename)
                    if result['result'] == 'ok':
                        zipf.write(image_filename, arcname=image_objectname)
        file_size = os.path.getsize(zip_filename)
        zip_object_name = f"{task_run_id}_{now}.zip"
        result = lanying_file_storage.upload(zip_object_name, zip_filename)
        if result['result'] == 'ok':
            update_task_run_field(app_id, task_run_id, "zip_file", zip_object_name)
            update_task_run_field(app_id, task_run_id, "file_size", file_size)
            incrby_service_usage(app_id, 'storage_size', file_size)
            return {'result': 'ok'}
        else:
            return {'result': 'error', 'message': 'fail to make zip file'}
    except Exception as e:
        logging.exception(e)
        return {'result': 'error', 'message': 'fail to make zip file'}

def get_task_run_result_list(app_id, task_run_id):
    run_result_key = get_task_run_result_key(app_id, task_run_id)
    redis = lanying_redis.get_redis_connection()
    result_list = lanying_redis.redis_hvals(redis, run_result_key)
    dtos = []
    for result in result_list:
        dtos.append(json.loads(result))
    return {
        'result': 'ok',
        'data': {
            'list': dtos
        }
    }

def deploy_task_run(app_id, task_run_id):
    task_run = get_task_run(app_id, task_run_id)
    if task_run is None:
        return {'result': 'error', 'message': 'task_run not exist'}
    if task_run['status'] != 'success':
        return {'result': 'error', 'message': 'task_run status cannot deploy'}
    if task_run['deploy_status'] not in ["wait", "error", "success"]:
        return {'result': 'error', 'message': 'task_run deploy_status cannot deploy'}
    if 'zip_file' not in task_run:
        return {'result': 'error', 'message': 'zip file not exist'}
    from lanying_tasks import grow_ai_deply_task_run
    grow_ai_deply_task_run.apply_async(args = [app_id, task_run_id])
    return {'result': 'ok', 'data':{
        'success': True
    }}

def do_deploy_task_run(app_id, task_run_id, has_retry_times):
    try:
        update_task_run_field(app_id, task_run_id, "deploy_status", "running")
        result = do_deploy_task_run_internal(app_id, task_run_id, has_retry_times)
        if result['result'] == 'error':
            logging.info(f"do_deploy_task_run result | {result}")
            increase_task_run_field(app_id, task_run_id, "deploy_fail_times", 1)
            update_task_run_field(app_id, task_run_id, "deploy_error_message", result['message'])
            retry = result.get('retry', True)
            if retry:
                if has_retry_times:
                    update_task_run_field(app_id, task_run_id, "deploy_status", "retry")
                else:
                    update_task_run_field(app_id, task_run_id, "deploy_status", "error")
                raise Exception(result['message'])
            else:
                update_task_run_field(app_id, task_run_id, "deploy_status", "error")
                return result
        elif result['result'] == 'ok':
            increase_task_run_field(app_id, task_run_id, "deploy_success_times", 1)
            update_task_run_field(app_id, task_run_id, "deploy_status", "success")
            update_task_run_field(app_id, task_run_id, "deploy_error_message", '')
        return result
    except Exception as e:
        increase_task_run_field(app_id, task_run_id, "deploy_fail_times", 1)
        error_msg = 'internal error'
        try:
            error_msg = str(e.args[0])[:100]
        except Exception as ee:
            pass
        update_task_run_field(app_id, task_run_id, "deploy_error_message", error_msg)
        if has_retry_times:
            update_task_run_field(app_id, task_run_id, "deploy_status", "retry")
        else:
            update_task_run_field(app_id, task_run_id, "deploy_status", "error")
        raise e

def do_deploy_task_run_internal(app_id, task_run_id, has_retry_times):
    logging.info(f"deploy task_run start | app_id:{app_id}, task_run_id:{task_run_id}, has_retry_times:{has_retry_times}")
    timestr = datetime.now().strftime('%Y%m%d%H%M%S')
    datestr = datetime.now().strftime('%Y%m%d')
    task_run = get_task_run(app_id, task_run_id)
    if task_run is None:
        return {'result': 'error', 'message': 'task_run not exist'}
    if task_run['status'] != 'success':
        return {'result': 'error', 'message': 'task_run status cannot deploy'}
    if 'zip_file' not in task_run:
        return {'result': 'error', 'message': 'zip file not exist'}
    task_id = task_run['task_id']
    task = get_task(app_id, task_id)
    if task is None:
        return {'result': 'error', 'message': 'task not exist'}
    deploy = task['deploy']
    deploy_type = deploy.get('type', 'none')
    if deploy_type not in ["gitbook"]:
        return {'result': 'error', 'message': 'deploy type not support', 'retry': False}
    github_url = deploy.get('gitbook_url', '')
    fields = github_url.split("/")
    if len(fields) < 5 or fields[2] != 'github.com':
        return {'result': 'error', 'message': 'deploy config is bad'}
    github_owner = fields[3]
    github_repo = fields[4]
    github_token = deploy.get('gitbook_token', '')
    if len(github_token) == 0:
        return {'result': 'error', 'message': 'deploy token is bad'}
    github_api_url = f"https://api.github.com/repos/{github_owner}/{github_repo}"
    base_branch = deploy.get('gitbook_base_branch', 'master')
    base_dir = deploy.get('gitbook_base_dir', '/').strip("/")
    target_dir = deploy.get('gitbook_target_dir', '/').strip("/")
    target_relative_dir = os.path.relpath(target_dir,base_dir)
    new_branch = f"grow-ai-{task_run_id}-{timestr}"
    headers = {
        "Authorization": f"token {github_token}",
        "Accept": "application/vnd.github.v3+json"
    }
    # 获取基础分支的最后一次提交SHA
    response = requests.get(f"{github_api_url}/git/refs/heads/{base_branch}", headers=headers)
    if response.status_code != 200:
        return {'result': 'error', 'message': 'github get branch info failed'}
    commit_sha = response.json()["object"]["sha"]
    zip_object_name = task_run['zip_file']
    zip_filename = lanying_utils.get_temp_filename(app_id, ".zip")
    result = lanying_file_storage.download(zip_object_name, zip_filename)
    if result['result'] == 'error':
        logging.info(f"github response | {response.content}")
        return {'result': 'error', 'message': 'fail to download zip file'}
    summary_url = f"https://api.github.com/repos/{github_owner}/{github_repo}/contents/{base_dir}/SUMMARY.md?ref={commit_sha}"
    # 发送 GET 请求获取文件内容
    response = requests.get(summary_url, headers=headers)
    if response.status_code != 200:
        logging.info(f"github response | {response.content}")
        return {'result': 'error', 'message': 'github SUMMARY.md not found'}
    file_info = response.json()
    summary_text = base64.b64decode(file_info['content']).decode('utf-8')
    # 创建新分支
    data = {
        "ref": f"refs/heads/{new_branch}",
        "sha": commit_sha
    }
    response = requests.post(f"{github_api_url}/git/refs", headers=headers, json=data)
    if response.status_code != 201:
        logging.info(f"github response | {response.content}")
        return {'result': 'error', 'message': 'github create branch failed'}
    # 获取基础分支的树对象SHA
    response = requests.get(f"{github_api_url}/git/trees/{commit_sha}", headers=headers)
    if response.status_code != 200:
        logging.info(f"github response | {response.content}")
        return {'result': 'error', 'message': 'github get sha failed'}
    base_tree_sha = response.json()["sha"]
    tree = []
    summary_link_list = []
    with zipfile.ZipFile(zip_filename, 'r') as zip_ref:
        file_list = zip_ref.namelist()
        for filename in file_list:
            with zip_ref.open(filename) as file:
                bytes = file.read()
                base64_content = base64.b64encode(bytes).decode()
                blob_data = {
                    "content": base64_content,
                    "encoding": "base64"
                }
                response = requests.post(f"{github_api_url}/git/blobs", headers=headers, json=blob_data)
                if response.status_code != 201:
                    logging.info(f"github response | {response.content}")
                    return {'result': 'error', 'message': 'github fail to add blobs'}
                blob_sha = response.json()["sha"]
                github_path = os.path.join(target_dir, datestr, filename)
                link_path = os.path.join(target_relative_dir, datestr, filename)
                logging.info(f"blob data | filename:{filename}, github_path:{github_path}, sha:{blob_sha}")
                if filename.endswith(".md"):
                    content = bytes.decode()
                    title = content.splitlines()[0].strip().lstrip('#').strip()
                    summary_link_list.append(f"    * [{title}]({link_path})")
                tree.append({
                    "path": github_path,
                    "mode": "100644",
                    "type": "blob",
                    "sha": blob_sha
                })
    title1 = os.path.join(target_relative_dir, "README.md")
    title2 = os.path.join(target_relative_dir, datestr, "README.md")
    summary_list = summary_text.splitlines()
    found_title1 = False
    for line in summary_list:
        if title1 in line:
            found_title1 = True
    if not found_title1:
        summary_list.append(f"  * [{target_relative_dir}]({title1})")
    found_title2 = False
    for line in summary_list:
        if title2 in line:
            found_title2 = True
    if not found_title2:
        summary_list_new = []
        found_title1 = False
        for line in summary_list:
            if not found_title1:
                if title1 in line:
                    found_title1 = True
                    summary_list_new.append(line)
                    summary_list_new.append(f"  * [{datestr}]({title2})")
                    readme_content = f"# {datestr}"
                    readme_content_base64 = base64.b64encode(readme_content.encode()).decode()
                    blob_data = {
                    "content": readme_content_base64,
                    "encoding": "base64"
                    }
                    response = requests.post(f"{github_api_url}/git/blobs", headers=headers, json=blob_data)
                    if response.status_code != 201:
                        logging.info(f"github response | {response.content}")
                        return {'result': 'error', 'message': 'github fail to add date blobs'}
                    blob_sha = response.json()["sha"]
                    github_path = f"{target_dir}/{datestr}/README.md"
                    logging.info(f"blob data | filename:{filename}, github_path:{github_path}, sha:{blob_sha}")
                    tree.append({
                        "path": github_path,
                        "mode": "100644",
                        "type": "blob",
                        "sha": blob_sha
                    })
                else:
                    summary_list_new.append(line)
            else:
                summary_list_new.append(line)
        summary_list = summary_list_new
    found_title1 = False
    found_title2 = False
    summary_result = []
    for line in summary_list:
        summary_result.append(line)
        if not found_title1:
            if title1 in line:
                found_title1 = True
        elif not found_title2:
            if title2 in line:
                found_title2 = True
                for link in summary_link_list:
                    summary_result.append(link)
    new_summary_content = '\n'.join(summary_result)
    # logging.info(f"new_summary_content: {new_summary_content}")
    new_summary_content_base64 = base64.b64encode(new_summary_content.encode()).decode()
    blob_data = {
                    "content": new_summary_content_base64,
                    "encoding": "base64"
    }
    response = requests.post(f"{github_api_url}/git/blobs", headers=headers, json=blob_data)
    if response.status_code != 201:
        logging.info(f"github response | {response.content}")
        return {'result': 'error', 'message': 'github fail to add summary blobs'}
    blob_sha = response.json()["sha"]
    github_path = f"{base_dir}/SUMMARY.md"
    logging.info(f"blob data | filename:{filename}, github_path:{github_path}, sha:{blob_sha}")
    tree.append({
        "path": github_path,
        "mode": "100644",
        "type": "blob",
        "sha": blob_sha
    })
    # 创建新的树对象
    data = {
        "base_tree": base_tree_sha,
        "tree": tree
    }
    response = requests.post(f"{github_api_url}/git/trees", headers=headers, json=data)
    if response.status_code != 201:
        logging.info(f"github response | {response.content}")
        return {'result': 'error', 'message': 'github fail to create new tree object'}
    new_tree_sha = response.json()["sha"]
    # 创建新的提交对象
    commit_message = f"Grow AI deploy: {task_run_id}"
    data = {
        "message": commit_message,
        "parents": [commit_sha],
        "tree": new_tree_sha
    }
    response = requests.post(f"{github_api_url}/git/commits", headers=headers, json=data)
    if response.status_code != 201:
        logging.info(f"github response | {response.content}")
        return {'result': 'error', 'message': 'github fail to create commit'}
    new_commit_sha = response.json()["sha"]
    # 更新新分支的引用，使其指向新的提交
    data = {
        "sha": new_commit_sha
    }
    response = requests.patch(f"{github_api_url}/git/refs/heads/{new_branch}", headers=headers, json=data)
    if response.status_code != 200:
        logging.info(f"github response | {response.content}")
        return {'result': 'error', 'message': 'github fail to move commit'}
    # 提交Pull Request
    title = f"Grow AI PR: {task_run_id}"
    body = f"Grow AI PR: {task_run_id}"
    pr_data = {
        "title": title,
        "body": body,
        "head": new_branch,
        "base": base_branch
    }
    response = requests.post(f"{github_api_url}/pulls", headers=headers, json=pr_data)
    if response.status_code != 201:
        logging.info(f"github response | {response.content}")
        return {'result': 'error', 'message': 'github fail to commit PR'}
    pr_url = response.json().get("html_url")
    update_task_run_field(app_id, task_run_id, "pr_url", pr_url)
    logging.info(f"deploy task_run success | app_id:{app_id}, task_run_id:{task_run_id}, has_retry_times:{has_retry_times}, pr_url:{pr_url}")
    return {'result': 'ok', 'data':{
        'pr_url': pr_url
    }}

def task_run_retry(app_id, task_run_id):
    logging.info(f"task_run_retry start | app_id:{app_id}, task_run_id:{task_run_id}")
    now = int(time.time())
    task_run = get_task_run(app_id, task_run_id)
    if task_run is None:
        return {'result': 'error', 'message': 'task_run not exist'}
    if task_run['status'] != 'error':
        return {'result': 'error', 'message': 'task_run status cannot retry'}
    update_task_run_field(app_id, task_run_id, "status", "wait")
    update_task_run_field(app_id, task_run_id, "update_time", now)
    set_admin_token(app_id)
    from lanying_tasks import grow_ai_run_task
    grow_ai_run_task.apply_async(args = [app_id, task_run_id], countdown=2)
    return {'result': 'ok', 'data':{'success': True}}

def get_download_file(file_sign):
    redis = lanying_redis.get_redis_connection()
    key = make_file_sign_key(file_sign)
    object_name = lanying_redis.redis_get(redis, key)
    if object_name is None:
        return {'result': 'error', 'message': 'file not exist'}
    filename = lanying_utils.get_temp_filename("none", "") + object_name
    result = lanying_file_storage.download(object_name, filename)
    if result['result'] == 'error':
        return {'result': 'error', 'message': 'file not exist'}
    return {
        'result': 'ok',
        'data':{
            'file_path': filename,
            'object_name': object_name
        }
    }

def download_task_run_result(app_id, task_run_id):
    task_run = get_task_run(app_id, task_run_id)
    if task_run is None:
        return {'result': 'error', 'message': 'task_run not exist'}
    if 'zip_file' not in task_run:
        return {'result': 'error', 'message': 'zip file not exist'}
    zip_file = task_run['zip_file']
    file_sign = f"s_{task_run_id}_{int(time.time()*1000000)}_{random.randint(1,100000000)}_{random.randint(1,100000000)}_{uuid.uuid4()}"
    redis = lanying_redis.get_redis_connection()
    key = make_file_sign_key(file_sign)
    redis.setex(key, 1800, zip_file)
    return {
        'result': 'ok',
        'data': {
            'file_sign': file_sign
        }
    }

def make_file_sign_key(file_sign):
    return f'lanying_connector:grow_ai:file_sign:{file_sign}'

def parse_keywords(keywords):
    keyword_list = []
    for keyword in re.split("[\r\n]{1,}", keywords):
        if len(keyword) > 0 and not keyword.isspace():
            keyword_list.append(keyword)
    return keyword_list

def do_run_task_article(app_id, task_run, task, article_id, chatbot_user_id, keyword):
    logging.info(f"do_run_task_article start | app_id:{app_id}, task_id:{task_run['task_id']}, task_run_id:{task_run['task_run_id']}, article_id:{article_id}, chatbot_user_id:{chatbot_user_id}, keyword:{keyword}")
    now = int(time.time())
    image_count = task['image_count']
    word_count_min = task['word_count_min']
    word_count_max = task['word_count_max']
    from_user_id = task_run['user_id']
    task_prompt = task['prompt']
    action_prompt = "请生成一篇markdown格式的文章，不要生成图片：\n"
    word_prompt = f'字数范围 {word_count_min} - {word_count_max} 字\n'
    image_placeholder_text = '[插图]'
    image_placeholder_prompt = f'需要包含有且只有 1 个的插图占位标记, 使用 {image_placeholder_text} 表示, 注意此占位符需要独占一行，且不要有加粗等格式修饰；\n' if image_count > 0 else ''
    subject_prompt = '' if task_prompt == '' else f'文章主题或产品和公司介绍为：{task_prompt}\n'
    keyword_prompt = f'文章标题关键词为：{keyword}\n'
    text_prompt = f'{action_prompt}{word_prompt}{image_placeholder_prompt}{keyword_prompt}{subject_prompt}'
    reset_prompt_ext = {'ai':{'reset_prompt': True}}
    clean_user_message_count(app_id, from_user_id)
    text_result = request_to_ai(app_id, from_user_id, chatbot_user_id, text_prompt, reset_prompt_ext)
    if text_result['result'] == 'error':
        return {'result':'error', 'message': 'failed to generate article text'}
    article_info = {
        'create_time': now,
        'article_id': article_id,
        'from_user_id': from_user_id,
        'to_user_id': chatbot_user_id
    }
    article_text = text_result['data']['messages'][0]['content']
    if len(article_text) < 100:
        article_ext = lanying_utils.safe_json_loads(text_result['data']['messages'][0]['ext'])
        has_error = False
        try:
            has_error = article_ext['ai']['result'] == 'error'
        except Exception as e:
            pass
        if has_error:
            return {'result': 'error', 'message': article_text}
        else:
            antispam_message = lanying_config.get_message_antispam(app_id)
            if article_text == antispam_message:
                return {'result': 'error', 'message': 'article text is blocked'}
            else:
                return {'result': 'error', 'message': 'article text too short'}
    if image_count > 0:
        image_prompt = '请为这篇文章生成一幅精美的插图。'
        image_result = request_to_ai(app_id, from_user_id, chatbot_user_id, image_prompt, {})
        image_attachment = lanying_utils.safe_json_loads(image_result['data']['messages'][0]['attachment'])
        if 'url' not in image_attachment:
            return {'result': 'error', 'message': 'failed to generate image'}
        url = image_attachment['url']
        config = get_dummy_lanying_connector(app_id)
        image_png_filename = lanying_utils.get_temp_filename(app_id, ".png")
        image_jpg_filename = image_png_filename + ".jpg"
        extra = {'image_type': '1'}
        result = lanying_im_api.download_url(config, app_id, chatbot_user_id, url, image_png_filename, extra)
        if result['result'] == 'error':
            return result
        lanying_image.png_to_jpg(image_png_filename, image_jpg_filename)
        image_object_name = f"{article_id}_{now}_1.jpg"
        result = lanying_file_storage.upload(image_object_name, image_jpg_filename)
        if result['result'] == 'error':
            return result
        article_info['image_file'] = image_object_name
        image_str = f'![]({image_object_name})'
        if image_placeholder_text in article_text:
            article_text = article_text.replace(f"***{image_placeholder_text}***", image_placeholder_text)
            article_text = article_text.replace(image_placeholder_text, image_str, 1)
            article_text = article_text.replace(image_placeholder_text, '')
        else:
            article_text = f"{article_text}\n{image_str}\n"
    markdown_filename = lanying_utils.get_temp_filename(app_id, ".md")
    with open(markdown_filename, 'w') as file:
        file.write(article_text)
    markdown_object_name = f"{article_id}_{now}.md"
    result = lanying_file_storage.upload(markdown_object_name, markdown_filename)
    if result['result'] == 'error':
        return result
    article_info['markdown_file'] = markdown_object_name
    article_info['summary'] = article_text[:100]
    logging.info(f"do_run_task_article success | app_id:{app_id}, task_id:{task_run['task_id']}, task_run_id:{task_run['task_run_id']}, article_id:{article_id}, chatbot_user_id:{chatbot_user_id}, keyword:{keyword}, article_info:{article_info}")
    return {'result': 'ok', 'article_info': article_info}

def request_to_ai(app_id, from_user_id, to_user_id, content, ext = {}):
    type = 1
    content_type = 0
    logging.info(f"Send message received, app_id:{app_id}, from={from_user_id} to={to_user_id} type={type}, content_type={content_type} content={content}")
    adminToken = get_admin_token(app_id)
    apiEndpoint = lanying_config.get_lanying_api_endpoint(app_id)
    if adminToken:
        try:
            logging.info(f"request_ai start | from={from_user_id} to={to_user_id} type={type}, content_type={content_type} content={content}")
            sendResponse = requests.post(apiEndpoint + '/ai/message/send',
                                        headers={'app_id': app_id, 'access-token': adminToken},
                                        json={'type':type,
                                            'from_user_id':from_user_id,
                                            'targets':[to_user_id],
                                            'content_type':content_type,
                                            'content': content,
                                            'attachment': '',
                                            'config': '',
                                            'ext': json.dumps(ext, ensure_ascii=False)})
            logging.info(f"request_ai finish | response_text: {sendResponse.text}")
            result = sendResponse.json()
            if result['code'] == 200:
                return {'result': 'ok', 'data': result['data']}
            else:
                return {'result': 'error', 'message': result['message']}
        except Exception as e:
            logging.exception(e)
            pass
    return {'result': 'error', 'message': 'internal error'}

def update_task_run_field(app_id, task_run_id, field, value):
    redis = lanying_redis.get_redis_connection()
    redis.hset(get_task_run_key(app_id, task_run_id), field, value)

def increase_task_run_field(app_id, task_run_id, field, value):
    redis = lanying_redis.get_redis_connection()
    return redis.hincrby(get_task_run_key(app_id, task_run_id), field, value)

def get_task_run(app_id, task_run_id):
    redis = lanying_redis.get_redis_connection()
    key = get_task_run_key(app_id, task_run_id)
    info = lanying_redis.redis_hgetall(redis, key)
    if "create_time" in info:
        dto = {}
        for key,value in info.items():
            if key in ['create_time', 'article_cursor', 'article_count', 'file_size']:
                dto[key] = int(value)
            else:
                dto[key] = value
        if 'deploy_status' not in dto:
            dto['deploy_status'] = 'wait'
        return dto
    return None

def get_task_run_list(app_id, task_id):
    redis = lanying_redis.get_redis_connection()
    task_run_ids = reversed(lanying_redis.redis_lrange(redis, get_task_run_list_key(app_id, task_id), 0, -1))
    task_run_list = []
    for task_run_id in task_run_ids:
        task_run_info = get_task_run(app_id, task_run_id)
        if task_run_info:
            task_run_list.append(task_run_info)
    return {
        'result': 'ok',
        'data':{
            'list': task_run_list
        }
    }

def get_task_run_key(app_id, task_run_id):
    return f"lanying_connector:grow_ai:task_run:{app_id}:{task_run_id}"

def get_task_run_list_key(app_id, task_id):
    return f"lanying_connector:grow_ai:task_run_list:{app_id}:{task_id}"

def generate_task_run_id(task_id):
    now = datetime.now()
    date_str = now.strftime('%Y%m%d')
    redis = lanying_redis.get_redis_connection()
    key = f"lanying_connector:grow_ai:task_run_id_generator:{task_id}:{date_str}"
    id = redis.incrby(key, 1)
    redis.expire(key, 86400 + 3600)
    return f"{task_id}_{date_str}_{id}"

def generate_dummy_user_id():
    redis = lanying_redis.get_redis_connection()
    for i in range(100):
        user_id = int(time.time()*1000000) | 0b1111
        key = f"lanying_connector:grow_ai:dummy_user_id:{user_id}"
        success = redis.setnx(key, 1)
        if success:
            redis.expire(key, 30)
            return user_id
        else:
            time.sleep(0.1)

def get_task_run_result_key(app_id, task_run_id):
    return f"lanying_connector:grow_ai:task_run_result:{app_id}:{task_run_id}"

def clean_user_message_count(app_id, from_user_id):
    now = datetime.now()
    key = f"lanying:connector:message_per_month_per_user:{app_id}:{from_user_id}:{now.year}:{now.month}"
    redis = lanying_redis.get_redis_connection()
    redis.delete(key)

def set_admin_token(app_id):
    redis = lanying_redis.get_redis_connection()
    config = lanying_config.get_lanying_connector(app_id)
    if config:
        key = admin_token_key(app_id)
        redis.set(key, config.get('lanying_admin_token', ''))

def get_admin_token(app_id):
    redis = lanying_redis.get_redis_connection()
    key = admin_token_key(app_id)
    return lanying_redis.redis_get(redis, key)

def get_dummy_lanying_connector(app_id):
    return {
        'lanying_admin_token': get_admin_token(app_id)
    }

def admin_token_key(app_id):
    return f"lanying_connector:grow_ai:admin_token:{app_id}"
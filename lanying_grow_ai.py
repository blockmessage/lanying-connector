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
import copy

class TaskSetting:
    def __init__(self, app_id, name, note, chatbot_id, prompt, keywords, word_count_min, word_count_max, image_count, article_count, cycle_type, cycle_interval, file_list, deploy, title_reuse, site_id_list, target_dir, commit_type, target_summary_dir):
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
        self.title_reuse = title_reuse
        self.site_id_list = site_id_list
        self.target_dir = target_dir
        self.commit_type = commit_type
        self.target_summary_dir = target_summary_dir

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
            'deploy': json.dumps(self.deploy, ensure_ascii=False),
            'title_reuse': self.title_reuse,
            'site_id_list': json.dumps(self.site_id_list, ensure_ascii=False),
            'target_dir': self.target_dir,
            'commit_type': self.commit_type,
            'target_summary_dir': self.target_summary_dir
        }

class SiteSetting:
    def __init__(self, app_id, name, type, github_url, github_token, github_base_branch, github_base_dir, footer_note, lanying_link, title, copyright, canonical_link, meta_keywords, baidu_token, official_website_url, google_token, max_latest_num):
        self.app_id = app_id
        self.name = name
        self.type = type
        self.github_url = github_url
        self.github_token = github_token
        self.github_base_branch = github_base_branch
        self.github_base_dir = github_base_dir
        self.footer_note = footer_note
        self.lanying_link = lanying_link
        self.title = title
        self.copyright = copyright
        self.canonical_link = canonical_link
        self.meta_keywords = meta_keywords
        self.baidu_token = baidu_token
        self.official_website_url = official_website_url
        self.google_token = google_token
        self.max_latest_num = max_latest_num

    def to_hmset_fields(self):
        return {
            'app_id': self.app_id,
            'name': self.name,
            'type': self.type,
            'github_url': self.github_url,
            'github_token': self.github_token,
            'github_base_branch': self.github_base_branch,
            'github_base_dir': self.github_base_dir,
            'footer_note': self.footer_note,
            'lanying_link': self.lanying_link,
            'title': self.title,
            'copyright': self.copyright,
            'canonical_link': self.canonical_link,
            'meta_keywords': self.meta_keywords,
            'baidu_token': self.baidu_token,
            'official_website_url': self.official_website_url,
            'google_token': self.google_token,
            'max_latest_num': self.max_latest_num
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

def open_service(app_id, product_id, price, website_storage_limit, website_traffic_limit):
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
        'website_storage_limit': website_storage_limit,
        'website_traffic_limit': website_traffic_limit
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
            if key in ['create_time', 'product_id', 'article_num', 'storage_size', 'website_storage_limit', 'website_traffic_limit']:
                dto[key] = int(value)
            else:
                dto[key] = value
            if 'website_storage_limit' not in dto:
                dto['website_storage_limit'] = 0
            if 'website_traffic_limit' not in dto:
                dto['website_traffic_limit'] = 0
        return dto
    return None

def get_service_status_key(app_id):
    return f"lanying-connector:grow_ai:service_status:{app_id}"

def get_service_usage(app_id):
    article_num_key = get_service_statistic_key_list(app_id, 'article_num')[0]
    storage_size_key = get_service_statistic_key_list(app_id, 'storage_size')[0]
    website_storage_key = get_service_statistic_key_list(app_id, 'website_storage')[0]
    website_traffic_key = get_service_statistic_key_list(app_id, 'website_traffic')[0]
    redis = lanying_redis.get_redis_connection()
    article_num = redis.incrby(article_num_key, 0)
    storage_size = redis.incrby(storage_size_key, 0)
    website_storage = redis.incrby(website_storage_key, 0)
    website_traffic = redis.incrby(website_traffic_key, 0)
    return {
        'result': 'ok',
        'data':{
            'article_num': article_num,
            'storage_size': storage_size,
            'website_storage': website_storage,
            'website_traffic': website_traffic
        }
}

def incrby_service_usage(app_id, field, value):
    logging.info(f"incrby_service_usage | app_id:{app_id}, field:{field}, value:{value}")
    redis = lanying_redis.get_redis_connection()
    key_list = get_service_statistic_key_list(app_id, field)
    for key in key_list:
        redis.incrby(key, value)

def set_service_usage(app_id, field, value):
    logging.info(f"set_service_usage | app_id:{app_id}, field:{field}, value:{value}")
    redis = lanying_redis.get_redis_connection()
    key_list = get_service_statistic_key_list(app_id, field)
    for key in key_list:
        redis.set(key, value)

def get_service_statistic_key_list(app_id, field):
    if field in ['storage_size']:
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
    month_start_date_str = month_start_date.strftime('%Y-%m-%d')
    now_date_str = now.strftime('%Y-%m-%d')
    if field in ['website_storage']:
        return [
            f'lanying-connector:grow_ai:staistic:{field}:app:{app_id}',
            f'lanying-connector:grow_ai:staistic:{field}:everyday:{app_id}:{product_id}:{now_date_str}'
        ]
    else:
        return [
            f'lanying-connector:grow_ai:staistic:{field}:pay_start_date:{app_id}:{product_id}:{pay_start_date_str}',
            f'lanying-connector:grow_ai:staistic:{field}:month_start_date:{app_id}:{product_id}:{month_start_date_str}',
            f'lanying-connector:grow_ai:staistic:{field}:everyday:{app_id}:{product_id}:{now_date_str}',
            f'lanying-connector:grow_ai:staistic:{field}:app:{app_id}'
        ]

def create_task(task_setting: TaskSetting):
    now = int(time.time())
    app_id = task_setting.app_id
    set_admin_token(app_id)
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
    return {
        'result': 'ok',
        'data': {
            'task_id': task_id
        }
    }

def configure_task(task_id, task_setting: TaskSetting):
    now = int(time.time())
    app_id = task_setting.app_id
    set_admin_token(app_id)
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
        if new_task_info['title_reuse'] == 'off':
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
            if len(task_info['site_id_list']) > 0:
                site_id = task_info['site_id_list'][0]
                site = get_site(app_id, site_id)
                if site and 'site_url' in site and len(site['site_url']) > 0:
                    task_info['site_url'] = site['site_url']
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
            if key in ['word_count_min', 'word_count_max', 'image_count', 'article_count',
                       'cycle_interval', 'create_time', 'article_cursor', "total_article_num"]:
                dto[key] = int(value)
            elif key in ["text_message_quota_usage", "image_message_quota_usage"]:
                dto[key] = float(value)
            elif key in ['file_list', 'deploy', 'site_id_list']:
                dto[key] = json.loads(value)
            else:
                dto[key] = value
        if 'schedule' not in info:
            dto['schedule'] = 'on'
        if 'file_list' not in info:
            dto['file_list'] = []
        if 'deploy' not in info:
            dto['deploy'] = {'type': 'none'}
        if 'text_message_quota_usage' not in dto:
            dto['text_message_quota_usage'] = 0.0
        if 'image_message_quota_usage' not in dto:
            dto['image_message_quota_usage'] = 0.0
        if 'title_reuse' not in dto:
            dto['title_reuse'] = 'off'
        if 'site_id_list' not in dto:
            dto['site_id_list'] = []
        if 'target_dir' not in dto:
            dto['target_dir'] = dto.get('deploy',{}).get('gitbook_target_dir', '/articles')
        if 'commit_type' not in dto:
            dto['commit_type'] = dto.get('deploy',{}).get('commit_type', 'pull_request')
        if 'target_summary_dir' not in dto:
            dto['target_summary_dir'] = ''
        return dto
    return None

def update_task_field(app_id, task_id, field, value):
    redis = lanying_redis.get_redis_connection()
    redis.hset(get_task_key(app_id, task_id), field, value)

def increase_task_field(app_id, task_id, field, value):
    redis = lanying_redis.get_redis_connection()
    return redis.hincrby(get_task_key(app_id, task_id), field, value)

def increase_task_field_by_float(app_id, task_id, field, value):
    redis = lanying_redis.get_redis_connection()
    return redis.hincrbyfloat(get_task_key(app_id, task_id), field, value)

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
        elif result['result'] == 'continue':
            from lanying_tasks import grow_ai_run_task
            grow_ai_run_task.apply_async(args = [app_id, task_run_id], countdown=1)
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

def get_website_storage_limit(app_id):
    return lanying_config.get_app_config_int_from_redis(app_id, 'lanying_connector.grow_ai_website_storage_limit')

def get_website_traffic_limit(app_id):
    return lanying_config.get_app_config_int_from_redis(app_id, 'lanying_connector.grow_ai_website_traffic_limit')

def find_title(app_id, task_id, task_run_id, keywords, title_reuse):
    article_cursor = increase_task_field(app_id, task_id, 'article_cursor', 0)
    max = len(keywords)
    if title_reuse == 'off':
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
    else:
        if article_cursor >= max:
            update_task_field(app_id, task_id, "article_cursor", 0)
            article_cursor = 0
        title = keywords[article_cursor]
        set_article_title_used(app_id, task_id, title, task_run_id)
        increase_task_field(app_id, task_id, 'article_cursor', 1)
        return {
                'result': 'ok',
                'data':{
                    'title': title
                }
            }

def set_article_title_used(app_id, task_id, title, task_run_id):
    redis = lanying_redis.get_redis_connection()
    key = article_title_used_key(app_id, task_id)
    redis.hset(key, title, task_run_id)

def del_article_title_used(app_id, task_id, title):
    redis = lanying_redis.get_redis_connection()
    key = article_title_used_key(app_id, task_id)
    redis.hdel(key, title)

def is_article_title_used(app_id, task_id, title):
    redis = lanying_redis.get_redis_connection()
    key = article_title_used_key(app_id, task_id)
    return redis.hexists(key, title)

def article_title_used_key(app_id, task_id):
    return f'lanying_connector:grow_ai:article_title_used:{app_id}:{task_id}'

def set_article_title_statistic(app_id, task_id, type, title, value):
    redis = lanying_redis.get_redis_connection()
    key = article_title_statistic_key(app_id, task_id, type)
    redis.hset(key, title, value)

def incr_article_title_statistic(app_id, task_id, type, title, value):
    redis = lanying_redis.get_redis_connection()
    key = article_title_statistic_key(app_id, task_id, type)
    return redis.hincrby(key, title, value)

def del_article_title_statistic(app_id, task_id, type, title):
    redis = lanying_redis.get_redis_connection()
    key = article_title_statistic_key(app_id, task_id, type)
    redis.hdel(key, title)

def get_article_title_statistic(app_id, task_id, type, title):
    redis = lanying_redis.get_redis_connection()
    key = article_title_statistic_key(app_id, task_id, type)
    return lanying_redis.redis_hget(redis, key, title)

def article_title_statistic_key(app_id, task_id, type):
    return f'lanying_connector:grow_ai:article_title_{type}:{app_id}:{task_id}'

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
    article_generate_num = 0
    max_article_generate_num = 5
    start_from = task_run['start_from']
    for i in range(start_from, article_count):
        logging.info(f"do_run_task_internal for article | app_id:{app_id}, task_id:{task_id}, task_run_id:{task_run_id}, i:{i}")
        article_id = f'{task_run_id}_{i+1}'
        if redis.hexists(run_result_key, article_id):
            continue
        result = find_title(app_id, task_id, task_run_id, keywords, task['title_reuse'])
        if result['result'] == 'error':
            if result['message'] == 'article titles are exhausted':
                if cycle_type == 'none' and i > 0:
                    break
                elif cycle_type == 'cycle':
                    set_task_schedule(app_id, task_id, "off", result['message'])
            make_task_run_result_zip_file(app_id, task_run_id)
            return result
        keyword = result['data']['title']
        result = do_run_task_article(app_id, task_run, task, article_id, chatbot_user_id, keyword)
        if result['result'] == 'error':
            logging.info(f"do_run_task error | app_id:{app_id}, task_run_id:{task_run_id}, article_id:{article_id}, keyword:{keyword}, result:{result}")
            if result['message'] == 'quota_not_enough':
                if cycle_type == 'cycle':
                    set_task_schedule(app_id, task_id, "off", result['message'])
            make_task_run_result_zip_file(app_id, task_run_id)
            return result
        article_info = result['article_info']
        redis.hset(run_result_key, article_id, json.dumps(article_info, ensure_ascii=False))
        increase_task_run_field(app_id, task_run_id, "article_success_count", 1)
        incrby_service_usage(app_id, 'article_num', 1)
        increase_task_field(app_id, task_id, "total_article_num", 1)
        article_generate_num += 1
        update_task_run_field(app_id, task_run_id, "start_from", i+1)
        if article_generate_num >= max_article_generate_num and i < article_count - 1:
            logging.info(f"do_run_task_internal partially finish | app_id:{app_id}, task_run_id:{task_run_id}, progress:{i+1}/{article_count}")
            return {'result': 'continue'}
    result = make_task_run_result_zip_file(app_id, task_run_id)
    if result['result'] == 'error':
        return result
    logging.info(f"do_run_task finish | app_id:{app_id}, task_run_id:{task_run_id}")
    site_list = get_task_site_list(task)
    if site_list != []:
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
            old_file_size = task_run.get('file_size', 0)
            update_task_run_field(app_id, task_run_id, "file_size", file_size)
            incrby_service_usage(app_id, 'storage_size', file_size - old_file_size)
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
        if 'zip_file' not in task_run:
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

def get_task_site_list(task):
    site_id_list = task['site_id_list']
    site_list = []
    for site_id in site_id_list:
        site = get_site(task['app_id'], site_id)
        if site:
            site_list.append(site)
    return site_list

def parse_dir(dir, base_dir):
    new_dir = dir.strip('').rstrip('/')
    if os.path.isabs(new_dir):
        return new_dir, new_dir.lstrip('/')
    else:
        new_dir = os.path.join(base_dir, new_dir)
        return new_dir, new_dir.lstrip('/')

def find_title_from_content(content):
    match = re.search(r'^(#|title:) (.*)', content, re.MULTILINE)
    if match:
        return match.group(2).strip('" ')
    else:
        return '无标题'

def find_content_meta_key(content, key, default):
    pattern = r'^{}: (.*)'.format(key)
    match = re.search(pattern, content, re.MULTILINE)
    if match:
        return match.group(1).strip('" ')
    else:
        return default

def do_deploy_task_run_internal(app_id, task_run_id, has_retry_times):
    logging.info(f"deploy task_run start | app_id:{app_id}, task_run_id:{task_run_id}, has_retry_times:{has_retry_times}")
    timestr = datetime.now().strftime('%Y%m%d%H%M%S')
    task_run = get_task_run(app_id, task_run_id)
    if task_run is None:
        return {'result': 'error', 'message': 'task_run not exist'}
    if task_run['status'] != 'success':
        if 'zip_file' not in task_run:
            return {'result': 'error', 'message': 'task_run status cannot deploy'}
    if 'zip_file' not in task_run:
        return {'result': 'error', 'message': 'zip file not exist'}
    task_id = task_run['task_id']
    task = get_task(app_id, task_id)
    if task is None:
        return {'result': 'error', 'message': 'task not exist'}
    site_list = get_task_site_list(task)
    if site_list == []:
        return {'result': 'error', 'message': 'no site to deploy', 'retry': False}
    site = site_list[0]
    max_latest_num = site['max_latest_num']
    github_url = site.get('github_url', '')
    result = parse_github_url(github_url)
    if result['result'] == 'error':
        return result
    github_owner = result['github_owner']
    github_repo = result['github_repo']
    github_token = site.get('github_token', '')
    if len(github_token) == 0:
        return {'result': 'error', 'message': 'deploy token is bad'}
    commit_type = task.get('commit_type', 'pull_request')
    github_api_url = f"https://api.github.com/repos/{github_owner}/{github_repo}"
    base_branch = site.get('github_base_branch', 'master')
    abs_base_dir, base_dir = parse_dir(site.get('github_base_dir', '/'), '/')
    abs_target_dir, target_dir = parse_dir(task['target_dir'], abs_base_dir)
    target_relative_dir = os.path.relpath(abs_target_dir,abs_base_dir)
    if target_relative_dir == '.':
        target_relative_dir = ''
    target_summary_dir_abs_or_relative = task['target_dir'] if task['target_summary_dir'] == '' else task['target_summary_dir']
    abs_target_summary_dir, target_summary_dir = parse_dir(target_summary_dir_abs_or_relative, abs_base_dir)
    target_summary_relative_dir = os.path.relpath(abs_target_summary_dir,abs_base_dir)
    if target_summary_relative_dir == '.':
        target_summary_relative_dir = ''
    logging.info(f"do_deploy_task_run_internal dir: abs_base_dir:{abs_base_dir}, abs_target_dir:{abs_target_dir}, abs_target_summary_dir:{abs_target_summary_dir},target_relative_dir:{target_relative_dir}")
    if commit_type == 'pull_request':
        new_branch = f"grow-ai-{task_run_id}-{timestr}"
    else:
        new_branch = base_branch
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
    summary_file = os.path.join(base_dir, "SUMMARY.md")
    summary_url = f"https://api.github.com/repos/{github_owner}/{github_repo}/contents/{summary_file}?ref={commit_sha}"
    # 发送 GET 请求获取文件内容
    response = requests.get(summary_url, headers=headers)
    if response.status_code != 200:
        logging.info(f"github response | {response.content}")
        return {'result': 'error', 'message': 'github SUMMARY.md not found'}
    file_info = response.json()
    summary_text = base64.b64decode(file_info['content']).decode('utf-8')
    if commit_type == 'pull_request':
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
        def parse_article_index(x):
            pattern = re.compile(r'(\d{8})-(\d+)-(\d+)')
            match = pattern.search(x)
            if match:
                try:
                    return int(match.group(3))
                except Exception as e:
                    pass
            return 0
        sorted_file_list = sorted(file_list, key=parse_article_index)
        for filename in sorted_file_list:
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
                github_path = os.path.join(target_dir, filename)
                link_path = os.path.join(target_relative_dir, filename)
                logging.info(f"blob data | filename:{filename}, github_path:{github_path}, sha:{blob_sha}")
                if filename.endswith(".md"):
                    content = bytes.decode()
                    title = find_title_from_content(content)
                    summary_link_list.append({'title': title, 'link': link_path})
                    # summary_link_list.append(f"    * [{title}]({link_path})")
                tree.append({
                    "path": github_path,
                    "mode": "100644",
                    "type": "blob",
                    "sha": blob_sha
                })
    target_link = os.path.join(target_summary_relative_dir, "README.md")
    summary = GitBookSummary(summary_text = summary_text)
    if not summary.has_link(target_link):
        summary.append_summary(target_summary_relative_dir, target_link)
        readme_content = f"# {target_summary_dir}"
        readme_content_base64 = base64.b64encode(readme_content.encode()).decode()
        blob_data = {
        "content": readme_content_base64,
        "encoding": "base64"
        }
        response = requests.post(f"{github_api_url}/git/blobs", headers=headers, json=blob_data)
        if response.status_code != 201:
            logging.info(f"github response | {response.content}")
            return {'result': 'error', 'message': 'github fail to add target_dir blobs'}
        blob_sha = response.json()["sha"]
        github_path = os.path.join(target_summary_dir, "README.md")
        logging.info(f"blob data | github_path:{github_path}, sha:{blob_sha}")
        tree.append({
            "path": github_path,
            "mode": "100644",
            "type": "blob",
            "sha": blob_sha
        })
    target_summary = summary.get_summary_by_link(target_link)
    latest = 'latest'
    latest_title = '最新'
    latest_link = os.path.join(target_summary_relative_dir, latest, "README.md")
    if not summary.has_link(latest_link):
        summary.add_summary_link_after_parent(latest_title, latest_link, target_summary)
        readme_content = f"# {latest_title}"
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
        github_path = os.path.join(target_summary_dir, latest, "README.md")
        logging.info(f"blob data | github_path:{github_path}, sha:{blob_sha}")
        tree.append({
            "path": github_path,
            "mode": "100644",
            "type": "blob",
            "sha": blob_sha
        })
    latest_summary = summary.get_summary_by_link(latest_link)
    summary.add_summary_link_list_after_parent(summary_link_list, latest_summary)
    truncate_list = summary.truncate_summary(latest_summary, max_latest_num)
    if len(truncate_list) > 0:
        datestr = None
        for truncate_summary in truncate_list:
            truncate_summary_link = truncate_summary['link']
            date_pattern = re.compile(r'(?<!\d)(\d{8})(?!\d)')
            match = date_pattern.search(truncate_summary_link)
            if match:
                datestr = match.group(1)
                break
        if datestr is None:
            datestr = datetime.now().strftime('%Y%m%d')
        datestr_link = os.path.join(target_summary_relative_dir, datestr, "README.md")
        if not summary.has_link(datestr_link):
            summary.add_summary_link_after_brother(datestr, datestr_link, latest_summary)
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
            github_path = os.path.join(target_summary_dir, datestr, "README.md")
            logging.info(f"blob data | github_path:{github_path}, sha:{blob_sha}")
            tree.append({
                "path": github_path,
                "mode": "100644",
                "type": "blob",
                "sha": blob_sha
            })
        datestr_summary = summary.get_summary_by_link(datestr_link)
        summary.add_summary_link_list_after_parent(truncate_list, datestr_summary)
    new_summary_content = summary.to_markdown()
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
    github_path = os.path.join(base_dir, "SUMMARY.md")
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
    if commit_type == 'pull_request':
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
    else:
        pr_url = ''
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

def handle_ai_response_error(result, default_error_message, app_id, task_id, title):
    message = result['message']
    if message in ["rate_limit_reached", "no_quota", "quota_not_enough", "message_per_month_per_user_limit_reached", "deduct_failed", "service_is_expired"]:
        del_article_title_used(app_id, task_id, title)
    elif 'http_request_fail' in result and result['http_request_fail']:
        del_article_title_used(app_id, task_id, title)
    else:
        failed_times = incr_article_title_statistic(app_id, task_id, "failed_times", title, 1)
        if failed_times <= 3:
            logging.info(f"handle_ai_response_error | failed_times:{failed_times}, app_id:{app_id}, task_id:{task_id}, title:{title}, so retry title")
            del_article_title_used(app_id, task_id, title)
        else:
            logging.info(f"handle_ai_response_error | failed_times:{failed_times}, app_id:{app_id}, task_id:{task_id}, title:{title}, so delete title")
    retry = True
    if message in ["rate_limit_reached", "no_quota", "quota_not_enough", "message_per_month_per_user_limit_reached", "deduct_failed", "service_is_expired"]:
        retry = False
    if message in ["no_quota", "quota_not_enough"]:
        return {"result":"error", "message": "quota_not_enough", "retry": retry}
    return {'result': 'error', 'message': default_error_message, "retry": retry}

def generate_article(app_id, task_id, task_run_id, keyword, from_user_id, chatbot_user_id, text_prompt, word_count_min, word_count_max):
    now_article_text = ''
    message_quota_usage = 0.0
    word_count_expect_min = word_count_min
    word_count_expect_max = word_count_max
    for i in range(3):
        if i == 0:
            prompt_ext = {
                'ai': {
                    "history_msg_size_max": 4096,
                    "max_tokens": 4096,
                    'reset_prompt': True
                }
            }
        else:
            prompt_ext = {
                'ai': {
                    "history_msg_size_max": 4096,
                    "max_tokens": 4096
                }
            }
        clean_user_message_count(app_id, from_user_id)
        logging.info(f"generate_article start | i={i}, app_id:{app_id}, task_run_id:{task_run_id}")
        text_result = request_to_ai(app_id, from_user_id, chatbot_user_id, text_prompt, prompt_ext)
        if text_result['result'] == 'error':
            return text_result
        article_text_message_quota_usage = text_result['data']['message_quota_usage']
        message_quota_usage += article_text_message_quota_usage
        increase_task_run_field_by_float(app_id, task_run_id, "text_message_quota_usage", article_text_message_quota_usage)
        increase_task_field_by_float(app_id, task_id, "text_message_quota_usage", article_text_message_quota_usage)
        article_text = text_result['data']['messages'][0]['content']
        ext = lanying_utils.safe_json_loads(text_result['data']['messages'][0].get('ext',''))
        ai = ext.get('ai', {})
        finish_reason = ai.get('finish_reason', '')
        logging.info(f"generate_article got article_text | finish_reason: {finish_reason}, i={i}, app_id:{app_id}, task_run_id:{task_run_id}, article_text_len={len(article_text)}, word_count_expect_min:{word_count_expect_min}, word_count_expect_max:{word_count_expect_max}")
        if len(article_text) < 100:
            antispam_message = lanying_config.get_message_antispam(app_id)
            if article_text == antispam_message:
                return {'result': 'error', 'message': 'article text is blocked'}
        now_article_text += article_text
        now_article_len = len(now_article_text)
        if finish_reason not in ['length', 'max_tokens']:
            break
        word_count_expect_min = word_count_min - now_article_len
        word_count_expect_max = word_count_max - now_article_len
        text_prompt = f"请接着上次的回答继续生成，直接输出内容，保持文章连贯，不要有多余内容。"
    article_url_prefix = find_content_meta_key(now_article_text, 'url', '')
    return {'result': 'ok', 'article_text': now_article_text, 'article_url_prefix': article_url_prefix,  "message_quota_usage": message_quota_usage}

def make_clean_url(url):
    # 将下划线替换成连字符
    url = url.replace('_', '-')
    
    # 使用正则表达式只保留小写字母、数字和连字符
    url = re.sub(r'[^a-z0-9-]', '', url)
    
    return url

def do_run_task_article(app_id, task_run, task, article_id, chatbot_user_id, keyword):
    dry_run = task.get('dry_run', 'off')
    task_run_id = task_run['task_run_id']
    task_id = task['task_id']
    logging.info(f"do_run_task_article start | app_id:{app_id}, task_id:{task_id}, task_run_id:{task_run_id}, article_id:{article_id}, chatbot_user_id:{chatbot_user_id}, keyword:{keyword}")
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
    meta_prompt = '请以三个短划线（---）作为边界的 YAML Front Matter 格式为文件添加元数据，注意不要用```yaml ```作为边界，字段有：title, description, keywords, url。字段要求如下：\n1. title： 文章标题。\n2. description: 文章描述，用于搜索引擎SEO。\n3. keywords: 搜索引擎SEO关键字，字符串类型，多个关键字请以英文逗号连接。\n4. url: 标题的英文URL路径表示,建议的URL需要根据标题生成，形式是三到五个英文单词，使用连字符而不是下划线，使用小写字母,请尽量不要多于5个英文单词，为减少单词数量可以适当考虑缩写。\n'
    subject_prompt = '' if task_prompt == '' else f'如果文章内容涉及以下公司或产品，可在适当的时机引用说明并推荐：{task_prompt}\n'
    keyword_prompt = f'文章标题必须为：{keyword}\n'
    text_prompt = f'{action_prompt}{word_prompt}{image_placeholder_prompt}{meta_prompt}{keyword_prompt}{subject_prompt}'
    clean_user_message_count(app_id, from_user_id)
    if dry_run == 'on':
        logging.info(f"dry_run generate_article text: app_id:{app_id}, task_id:{task_id}, task_run_id:{task_run_id}, article_id:{article_id}")
        time.sleep(5)
        text_result = {
            'result': 'ok',
            'article_text': f"# {keyword}\n{lanying_utils.generate_random_text(word_count_min)}",
            'message_quota_usage': 0.0
        }
    else:
        text_result = generate_article(app_id, task_id, task_run_id, keyword, from_user_id, chatbot_user_id, text_prompt, word_count_min, word_count_max)
    if text_result['result'] == 'error':
        return handle_ai_response_error(text_result, 'failed to generate article text', app_id, task_id, keyword)
    article_url_prefix = text_result['article_url_prefix']
    article_info = {
        'create_time': now,
        'article_id': article_id,
        'from_user_id': from_user_id,
        'to_user_id': chatbot_user_id,
        'text_message_quota_usage': text_result['message_quota_usage'],
        'article_url_prefix': text_result['article_url_prefix'],
        'title': keyword
    }
    article_text = text_result['article_text']
    if image_count > 0:
        image_prompt = '请为这篇文章生成一幅精美的插图。'
        if dry_run == 'on':
            logging.info(f"dry_run generate_article image: app_id:{app_id}, task_id:{task_id}, task_run_id:{task_run_id}, article_id:{article_id}")
            time.sleep(5)
            image_result = {
                'result': 'ok',
                'data':{
                    'messages':[
                        {'attachment': '{"url":"https://www.lanyingim.com/img/whitelogo-zh-sticky.png"}'}
                    ],
                    'message_quota_usage': 0.0
                }
            }
        else:
            image_prompt_ext = {
                'ai': {
                    "history_msg_size_max": 4096
                }
            }
            image_result = request_to_ai(app_id, from_user_id, chatbot_user_id, image_prompt, image_prompt_ext)
        if image_result['result'] == 'error':
            return handle_ai_response_error(image_result, 'failed to generate image', app_id, task_id, keyword)
        article_image_message_quota_usage = image_result['data']['message_quota_usage']
        increase_task_run_field_by_float(app_id, task_run_id, "image_message_quota_usage", article_image_message_quota_usage)
        increase_task_field_by_float(app_id, task_id, "image_message_quota_usage", article_image_message_quota_usage)
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
        article_info['image_message_quota_usage'] = article_image_message_quota_usage
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
    if article_url_prefix == '':
        markdown_object_name = make_clean_url(f"{article_id}-{now}") + ".md"
    else:
        markdown_object_name = make_clean_url(f"{article_url_prefix}-{article_id}-{now}") + ".md"
    result = lanying_file_storage.upload(markdown_object_name, markdown_filename)
    if result['result'] == 'error':
        return result
    article_info['markdown_file'] = markdown_object_name
    article_info['summary'] = article_text[:100]
    incr_article_title_statistic(app_id, task_id, "success_times", keyword, 1)
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
                return format_ai_message_result(result)
            else:
                return {'result': 'error', 'message': result['message'], 'http_request_fail': True}
        except Exception as e:
            logging.exception(e)
            return {'result': 'error', 'message': 'internal error', 'http_request_fail': True}
    return {'result': 'error', 'message': 'internal error'}

def format_ai_message_result(result):
    try:
        ext = lanying_utils.safe_json_loads(result['data']['messages'][0]['ext'])
        ai = ext['ai']
        if 'result' in ai and ai['result'] == 'error':
            error_code = ai['error_code']
            error_message = ai['error_message']
            logging.info(f"format_ai_message_result got error | code: {error_code}, message: {error_message}")
            return {'result': 'error', 'message': error_code if error_code != '' else error_message}
    except Exception as e:
        pass
    return {'result': 'ok', 'data': result['data']}

def update_task_run_field(app_id, task_run_id, field, value):
    redis = lanying_redis.get_redis_connection()
    redis.hset(get_task_run_key(app_id, task_run_id), field, value)

def increase_task_run_field(app_id, task_run_id, field, value):
    redis = lanying_redis.get_redis_connection()
    return redis.hincrby(get_task_run_key(app_id, task_run_id), field, value)

def increase_task_run_field_by_float(app_id, task_run_id, field, value):
    redis = lanying_redis.get_redis_connection()
    return redis.hincrbyfloat(get_task_run_key(app_id, task_run_id), field, value)

def get_task_run(app_id, task_run_id):
    redis = lanying_redis.get_redis_connection()
    key = get_task_run_key(app_id, task_run_id)
    info = lanying_redis.redis_hgetall(redis, key)
    if "create_time" in info:
        dto = {}
        for key,value in info.items():
            if key in ['create_time', 'article_cursor', 'article_count', 'file_size', 'start_from']:
                dto[key] = int(value)
            elif key in ["text_message_quota_usage", "image_message_quota_usage"]:
                dto[key] = float(value)
            else:
                dto[key] = value
        if 'deploy_status' not in dto:
            dto['deploy_status'] = 'wait'
        if 'text_message_quota_usage' not in dto:
            dto['text_message_quota_usage'] = 0.0
        if 'image_message_quota_usage' not in dto:
            dto['image_message_quota_usage'] = 0.0
        if 'start_from' not in dto:
            dto['start_from'] = 0
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

def release_finish(repository, release):
    logging.info(f"release_finish | repository={repository}, release:{release}")
    fields = repository.split('/')
    if len(fields) < 2:
        return {'result': 'error', 'message': 'bad repository'}
    github_owner = fields[0]
    github_repo = fields[1]
    site_id_list = get_github_site_id_list(github_owner, github_repo)
    owner_site_id = None
    owner_time = 0
    for site_id, app_id in site_id_list.items():
        site = get_site(app_id, site_id)
        if site:
            github_url = site.get('github_url', '')
            result = parse_github_url(github_url)
            if result['result'] == 'error':
                continue
            if result['github_owner'] == github_owner and result['github_repo'] == github_repo:
                update_time = site['update_time']
                if update_time > owner_time:
                    owner_time = update_time
                    owner_site_id = site_id
    if owner_site_id is not None:
        return start_deploy_github_action(app_id, '', owner_site_id, github_owner, github_repo, release)
    return {'result': 'error', 'message': 'deploy not found'}

def start_deploy_github_action(app_id, task_id, site_id, github_owner, github_repo, release):
    logging.info(f"start_deploy_github_action | app_id:{app_id}, task_id:{task_id}, site_id:{site_id}, github_owner:{github_owner}, github_repo:{github_repo}, release:{release}")
    deploy_repo_owner = 'maxim-top'
    deploy_repo_name = 'im.gitbook'
    deploy_workflow_id = 'deploy_sub_site.yml'
    deploy_github_token = os.getenv('GROW_AI_GITHUB_TOKEN', '')
    site_name = get_github_site(github_owner, github_repo)
    deploy_code = f"{uuid.uuid4()}-{int(time.time()*1000000)}"
    set_deploy_code(deploy_code, {
        'app_id': app_id,
        'task_id': task_id,
        'site_id': site_id,
        'github_owner': github_owner,
        'github_repo': github_repo
    })
    connector_server = lanying_utils.get_internet_connector_server()
    # 构建请求头和请求URL
    headers = {
        'Authorization': f'token {deploy_github_token}',
        'Accept': 'application/vnd.github.v3+json'
    }
    url = f'https://api.github.com/repos/{deploy_repo_owner}/{deploy_repo_name}/actions/workflows/{deploy_workflow_id}/dispatches'

    # 请求体内容
    data = {
        'ref': 'master',
        'inputs': {
            'book_url': f'https://github.com/{github_owner}/{github_repo}/releases/download/{release}/book.tar.gz',
            'oss_path': f'/{site_name}',
            'cdn_url': make_site_full_url(site_name),
            'check_url': f'{connector_server}/grow_ai/check_deploy?code={deploy_code}',
            'callback_url': f'{connector_server}/grow_ai/deploy_finish?code={deploy_code}'
        }
    }
    logging.info(f"start_deploy_github_action | url={url}, data={data}")

    # 发送POST请求
    response = requests.post(url, headers=headers, data=json.dumps(data))

    if response.status_code == 204:
        logging.info('Workflow dispatched successfully')
        return {'result': 'ok', 'data':{'success': True}}
    else:
        logging.info(f'Failed to dispatch workflow: {response.status_code}')
        logging.info(response.json())
        return {'result': 'error', 'message': 'failed to dispatch workflow'}

def make_site_full_url(site_name):
    return f'https://{site_name}.site.chatai101.com/'

def set_deploy_code(deploy_code, info):
    redis = lanying_redis.get_redis_connection()
    key = deploy_code_key(deploy_code)
    redis.setex(key, 3600, json.dumps(info, ensure_ascii=False))

def get_deploy_code(deploy_code):
    redis = lanying_redis.get_redis_connection()
    key = deploy_code_key(deploy_code)
    info = lanying_redis.redis_get(redis, key)
    if info:
        return json.loads(info)
    return None

def deploy_code_key(deploy_code):
    return f"lanying_connector:grow_ai:deploy_code:{deploy_code}"

def check_deploy(deploy_code, release_size):
    logging.info(f"check_deploy | deploy_code={deploy_code}, release_size:{release_size}")
    code_info = get_deploy_code(deploy_code)
    logging.info(f"deploy_finish code info:{code_info}")
    if code_info is None:
        return {'result': 'error', 'message': 'code not found'}
    app_id = code_info['app_id']
    site_id = code_info['site_id']
    site = get_site(app_id, site_id)
    if site is None:
        update_site_field(app_id, site_id, "deploy_result", "failed")
        update_site_field(app_id, site_id, "deploy_failed_reason", "site not found")
        return {'result': 'error', 'message': 'site not found'}
    service_status = get_service_status(app_id)
    if service_status is None:
        update_site_field(app_id, site_id, "deploy_result", "failed")
        update_site_field(app_id, site_id, "deploy_failed_reason", "service status not found")
        return {'result': 'error', 'message': 'service status not found'}
    website_storage_limit = service_status['website_storage_limit']
    product_id = service_status['product_id']
    if product_id == 9805:
        update_site_storage(app_id, site_id, release_size)
        return {'result': 'ok', 'data': {'success': True}}
    old_website_storage = site['website_storage']
    total_website_storage = get_app_total_website_storage(app_id)
    new_website_storage = total_website_storage - old_website_storage + release_size
    new_website_storage_mb = new_website_storage / 1024 / 1024
    logging.info(f"check_deploy calc website_storage | old_website_storage:{old_website_storage}, total_website_storage:{total_website_storage}, new_website_storage_mb:{new_website_storage_mb}")
    if new_website_storage_mb >= website_storage_limit:
        update_site_field(app_id, site_id, "deploy_result", "failed")
        update_site_field(app_id, site_id, "deploy_failed_reason", "website storage limit reached")
        return {'result': 'error', 'message': f'website storage limit reached : {new_website_storage_mb}/{website_storage_limit}'}
    else:
        update_site_storage(app_id, site_id, release_size)
        return {'result': 'ok', 'data': {'success': True}}

def update_site_storage(app_id, site_id, website_storage):
    update_site_field(app_id, site_id, 'website_storage', website_storage)
    total_website_storage = get_app_total_website_storage(app_id)
    set_service_usage(app_id, 'website_storage', total_website_storage)

def get_app_total_website_storage(app_id):
    site_list = get_site_list(app_id)['data']['list']
    total_website_storage = 0
    for site in site_list:
        total_website_storage += site['website_storage']
    return total_website_storage

def deploy_finish(deploy_code, deploy_result):
    logging.info(f"deploy_finish | deploy_code={deploy_code}, deploy_result:{deploy_result}")
    code_info = get_deploy_code(deploy_code)
    logging.info(f"deploy_finish code info:{code_info}")
    if code_info is None:
        return {'result': 'error', 'message': 'code not found'}
    app_id = code_info['app_id']
    site_id = code_info['site_id']
    site = get_site(app_id, site_id)
    if site is None:
        return {'result': 'error', 'message': 'site not found'}
    update_site_field(app_id, site_id, "deploy_result", "success")
    return {'result':'ok', 'data':{'success': True}}

def get_github_site(github_owner, github_repo):
    key = github_site_key(github_owner, github_repo)
    redis = lanying_redis.get_redis_connection()
    result = lanying_redis.redis_get(redis, key)
    if result:
        return result
    site_name_key = github_site_name_key()
    for i in range(1000):
        site_name = lanying_utils.generate_random_letters(6)
        result = redis.hsetnx(site_name_key, site_name, f'{github_owner}/{github_repo}')
        if result > 0:
            redis.set(key, site_name)
            return site_name
    raise Exception('fail to get github site')

def github_site_key(github_owner, github_repo):
    return f"lanying_connector:grow_ai:github_site:{github_owner}:{github_repo}"

def github_site_name_key():
    return f"lanying_connector:grow_ai:github_site_name"

def parse_github_url(github_url):
    if github_url.startswith("https://github.com/"):
        fields = github_url.split("/")
        if len(fields) < 5 or fields[2] != 'github.com':
            return {'result': 'error', 'message': 'github_url is bad'}
        github_owner = fields[3]
        github_repo = fields[4]
        if github_repo.endswith(".git"):
            github_repo = github_repo[:-4]
        return {'result': 'ok', 'github_owner': github_owner, "github_repo": github_repo}
    elif github_url.startswith("git@github.com:"):
        fields = re.split("[:/]{1,}", github_url)
        if len(fields) < 3:
            return {'result': 'error', 'message': 'github_url is bad'}
        github_owner = fields[1]
        github_repo = fields[2]
        if github_repo.endswith(".git"):
            github_repo = github_repo[:-4]
        return {'result': 'ok', 'github_owner': github_owner, "github_repo": github_repo}
    return {'result': 'error', 'message': 'github_url is bad'}


def create_site(site_setting: SiteSetting):
    now = int(time.time())
    result = check_site_setting(site_setting)
    if result['result'] == 'error':
        return result
    app_id = site_setting.app_id
    site_id = generate_site_id()
    redis = lanying_redis.get_redis_connection()
    fields = site_setting.to_hmset_fields()
    fields['status'] = 'normal'
    fields['create_time'] = now
    fields['site_id'] = site_id
    logging.info(f"create site start | app_id:{app_id}, site_info:{fields}")
    redis.hmset(get_site_key(app_id, site_id), fields)
    redis.rpush(get_site_list_key(app_id), site_id)
    site_info = get_site(app_id, site_id)
    logging.info(f"create site finish | app_id:{app_id}, site_info:{site_info}")
    maybe_register_github_site(app_id, site_info)
    maybe_sync_to_github({}, site_info)
    return {
        'result': 'ok',
        'data': {
            'site_id': site_id
        }
    }

def configure_site(site_id, site_setting: SiteSetting):
    now = int(time.time())
    result = check_site_setting(site_setting)
    if result['result'] == 'error':
        return result
    app_id = site_setting.app_id
    site_info = get_site(app_id, site_id)
    if site_info is None:
        return {'result': 'error', 'message': 'site_id not exist'}
    redis = lanying_redis.get_redis_connection()
    fields = site_setting.to_hmset_fields()
    fields['update_time'] = now
    logging.info(f"configure site start | app_id:{app_id}, site_info:{fields}")
    redis.hmset(get_site_key(app_id, site_id), fields)
    new_site_info = get_site(app_id, site_id)
    maybe_register_github_site(app_id, new_site_info)
    maybe_sync_to_github(site_info, new_site_info)
    return {
        'result': 'ok',
        'data': {
            'success': True
        }
    }

def maybe_sync_to_github(old_site, site):
    try:
        executor.submit(sync_to_github, site)
    except Exception as e:
        pass

def sync_to_github(site):
    github_url = site.get('github_url', '')
    github_token = site.get('github_token', '')
    result = parse_github_url(github_url)
    if result['result'] == 'error':
        return result
    github_owner = result['github_owner']
    github_repo = result['github_repo']
    github_api_url = f"https://api.github.com/repos/{github_owner}/{github_repo}"
    base_branch = site.get('github_base_branch', 'master')
    base_dir = site.get('github_base_dir', '/').strip("/")
    headers = {
        "Authorization": f"token {github_token}",
        "Accept": "application/vnd.github.v3+json"
    }
    book_file = os.path.join(base_dir, "book.json")
    book_url = f"{github_api_url}/contents/{book_file}"
    # 发送 GET 请求获取文件内容
    response = requests.get(book_url, headers=headers)
    if response.status_code != 200:
        logging.info(f"github response | {response.content}")
        return {'result': 'error', 'message': 'github book.json not found'}
    file_info = response.json()
    sha = file_info['sha']
    book_text = base64.b64decode(file_info['content']).decode('utf-8')
    try:
        book_json = json.loads(book_text)
    except Exception as e:
        return {'result': 'error', 'message': 'book.json is not json'}
    new_book_json = transform_site_to_book_json(site, book_json, github_owner, github_repo, base_branch)
    if new_book_json == book_json:
        return {'result': 'ok'}
    new_book_text = json.dumps(new_book_json, ensure_ascii=False, indent=4) + "\n"
    new_book_base64 = base64.b64encode(new_book_text.encode()).decode()
    update_data = {
        "message": "Update book.json from LanyingIM Console",
        "content": new_book_base64,
        "encoding": "base64",
        "sha": sha
    }
    response = requests.put(book_url, headers=headers, json=update_data)
    if response.status_code != 200:
        logging.info(f"github response | {response.content}")
        return {'result': 'error', 'message': 'github fail to commit'}
    return {'result': 'ok'}

def transform_site_to_book_json(site, book_json, github_owner, github_repo, base_branch):
    new_book_json = copy.deepcopy(book_json)
    for field in ['title', 'github_buttons', 'copyright', 'edit_link', 'logo_site_url', 'canonical_link', 'meta_keywords', 'baidu_token', 'footer_note', 'lanying_link', 'sitemap_hostname', 'google_token']:
        try:
            if field == 'title':
                title = site.get('title', '')
                if len(title) > 0:
                    new_book_json['title'] = title
            elif field == 'github_buttons':
                new_book_json['pluginsConfig']['github-buttons']['repo'] = f'{github_owner}/{github_repo}'
            elif field == 'copyright':
                copyright = site.get('copyright', '')
                if len(copyright) > 0:
                    official_website_url = site.get('official_website_url', '')
                    if len(official_website_url) > 0:
                        site_url = official_website_url
                    else:
                        site_url = '/'
                    new_book_json['pluginsConfig']['tbfed-pagefooter']['copyright'] = f"{copyright} | <a href='{site_url}' style='text-decoration:none!important;'>官网</a> | <a href='/sitemap.xml' style='text-decoration:none!important;' target='_blank'>网站地图</a>"
            elif field == 'edit_link':
                new_book_json['pluginsConfig']['edit-link']['base'] = f'https://github.com/{github_owner}/{github_repo}/blob/{base_branch}'
            elif field == 'logo_site_url':
                official_website_url = site.get('official_website_url', '')
                if len(official_website_url) > 0:
                    site_url = official_website_url
                else:
                    site_url = '/'
                new_book_json['pluginsConfig']['logo']['url'] = site_url
            elif field == 'canonical_link':
                canonical_link = site.get('canonical_link','')
                if len(canonical_link) == 0:
                    site_name = get_github_site(github_owner, github_repo)
                    canonical_link = make_site_full_url(site_name)
                new_canonical_link = canonical_link.rstrip('/')
                new_book_json['pluginsConfig']['canonical-link']['baseURL'] = new_canonical_link
            elif field == 'sitemap_hostname':
                canonical_link = site.get('canonical_link','')
                if len(canonical_link) > 0:
                    sitemap_hostname = canonical_link
                else:
                    site_name = get_github_site(github_owner, github_repo)
                    site_url = make_site_full_url(site_name)
                    sitemap_hostname = site_url
                new_book_json['pluginsConfig']['lanying-grow-ai']['sitemap_hostname'] = sitemap_hostname
            elif field == 'meta_keywords':
                meta_keywords = site.get('meta_keywords', '')
                if len(meta_keywords) > 0:
                    new_book_json['pluginsConfig']['meta']['data'][0]['content'] = meta_keywords
            elif field == 'baidu_token':
                baidu_token = site.get('baidu_token', '')
                if len(baidu_token) > 0:
                    new_book_json['pluginsConfig']['3-ba']['token'] = baidu_token
            elif field == 'google_token':
                google_token = site.get('google_token', '')
                if len(google_token) > 0:
                    new_book_json['pluginsConfig']['ga4']['tag'] = google_token
            elif field == 'footer_note':
                footer_note = site.get('footer_note', '')
                if len(footer_note) > 0:
                    new_book_json['pluginsConfig']['lanying-grow-ai']['footer_note'] = footer_note
            elif field == 'lanying_link':
                lanying_link = site.get('lanying_link', '')
                if len(lanying_link) > 0:
                    new_book_json['pluginsConfig']['lanying-grow-ai']['lanying_link'] = lanying_link
        except Exception as e:
            pass
    logging.info(f"transform_site_to_book_json | site:{site}, book_json:{book_json}, new_book_json:{new_book_json}")
    return new_book_json

def get_site_list(app_id):
    redis = lanying_redis.get_redis_connection()
    site_ids = reversed(lanying_redis.redis_lrange(redis, get_site_list_key(app_id), 0, -1))
    site_list = []
    for site_id in site_ids:
        site_info = get_site(app_id, site_id)
        if site_info:
            site_list.append(site_info)
    return {
        'result': 'ok',
        'data':
            {
                'list': site_list
            }
    }

def maybe_add_site_url(site_info):
    if site_info['type'] == 'gitbook':
        github_url = site_info['github_url']
        result = parse_github_url(github_url)
        if result['result'] == 'ok':
            github_owner = result['github_owner']
            github_repo = result['github_repo']
            site_name = get_github_site(github_owner, github_repo)
            site_url = make_site_full_url(site_name)
            site_info['site_url'] = site_url

def get_site(app_id, site_id):
    redis = lanying_redis.get_redis_connection()
    key = get_site_key(app_id, site_id)
    info = lanying_redis.redis_hgetall(redis, key)
    if "create_time" in info:
        dto = {}
        for key,value in info.items():
            if key in ['create_time', 'update_time', 'website_storage', 'max_latest_num']:
                dto[key] = int(value)
            else:
                dto[key] = value
        if 'update_time' not in dto:
            dto['update_time'] = dto['create_time']
        if 'website_storage' not in dto:
            dto['website_storage'] = 0
        if 'title' not in dto:
            dto['title'] = ''
        if 'copyright' not in dto:
            dto['copyright'] = ''
        if 'canonical_link' not in dto:
            dto['canonical_link'] = ''
        if 'meta_keywords' not in dto:
            dto['meta_keywords'] = ''
        if 'baidu_token' not in dto:
            dto['baidu_token'] = ''
        if 'official_website_url' not in dto:
            dto['official_website_url'] = ''
        if 'google_token' not in dto:
            dto['google_token'] = ''
        if 'max_latest_num' not in dto:
            dto['max_latest_num'] = 10
        maybe_add_site_url(dto)
        return dto
    return None

def update_site_field(app_id, site_id, field, value):
    redis = lanying_redis.get_redis_connection()
    redis.hset(get_site_key(app_id, site_id), field, value)

def generate_site_id():
    redis = lanying_redis.get_redis_connection()
    return redis.incrby("lanying_connector:grow_ai:site_id_generator", 1)

def get_site_key(app_id, site_id):
    return f"lanying_connector:grow_ai:site:{app_id}:{site_id}"

def get_site_list_key(app_id):
    return f"lanying_connector:grow_ai:site_list:{app_id}"

def check_site_setting(site_setting: SiteSetting):
    if site_setting.type not in ["gitbook"]:
        return {'result': 'error', 'message': 'invalid site type'}
    github_url = site_setting.github_url
    result = parse_github_url(github_url)
    if result['result'] == 'error':
        return result
    github_owner = result['github_owner']
    github_repo = result['github_repo']
    github_token = site_setting.github_token
    if len(github_token) == 0:
        return {'result': 'error', 'message': 'github token is bad'}
    github_api_url = f"https://api.github.com/repos/{github_owner}/{github_repo}"
    base_branch = site_setting.github_base_branch
    headers = {
        "Authorization": f"token {github_token}",
        "Accept": "application/vnd.github.v3+json"
    }
    # 获取基础分支的最后一次提交SHA
    response = requests.get(f"{github_api_url}/git/refs/heads/{base_branch}", headers=headers)
    if response.status_code != 200:
        return {'result': 'error', 'message': 'github token is bad'}
    return {'result': 'ok'}

def maybe_register_github_site(app_id, site_info):
    if site_info['type'] == 'gitbook':
        site_id = site_info['site_id']
        github_url = site_info['github_url']
        result = parse_github_url(github_url)
        if result['result'] == 'error':
            return result
        github_owner = result['github_owner']
        github_repo = result['github_repo']
        redis = lanying_redis.get_redis_connection()
        key = github_register_site_key(github_owner, github_repo)
        redis.hset(key, site_id, app_id)
        get_github_site(github_owner, github_repo)

def get_github_site_id_list(github_owner, github_repo):
    redis = lanying_redis.get_redis_connection()
    key = github_register_site_key(github_owner, github_repo)
    return lanying_redis.redis_hgetall(redis, key)

def github_register_site_key(github_owner, github_repo):
    return f"lanying_connector:grow_ai:github_repo_site:{github_owner}:{github_repo}"

def all_task():
    redis = lanying_redis.get_redis_connection()
    prefix = "lanying_connector:grow_ai:task:"
    keys = lanying_redis.redis_keys(redis, f"{prefix}*")
    for key in keys:
        fields = str(key)[len(prefix):].split(':')
        if len(fields) == 2:
            app_id = fields[0]
            task_id = fields[1]
            task = get_task(app_id, task_id)
            if task:
                print(task)

class GitBookSummary:
    def __init__(self, summary_text: str):
        self.next_id = 0
        self.summary_list = []
        for line in summary_text.splitlines():
            self.summary_list.append(self.parse_line(line))
    
    def set_summary_list(self, summary_list):
        self.summary_list = summary_list

    def parse_line(self, line):
        pattern = r'(\s*)\*\s*\[\s*(.+?)\s*\]\s*\(\s*(.+?)\s*\)'
        match = re.match(pattern, line)
        if match:
            space = match.group(1)
            title = match.group(2).strip()
            link = match.group(3).strip()
            return self.make_summary(space, title, link)
        else:
            self.next_id += 1
            return {'type': 'line', 'line': line, 'id': self.next_id}

    def make_summary(self, space, title, link):
        self.next_id += 1
        return {'type': 'link', 'space': space, 'title': title, 'link': link, 'id': self.next_id}
    
    def has_link(self, link):
        return self.get_summary_by_link(link) is not None
    
    def get_summary_by_link(self, link):
        for summary in self.summary_list:
            if summary['type'] == 'link' and summary['link'] == link:
                return summary
    
    def append_summary(self, title, link):
        self.summary_list.append(self.make_summary('', title, link))
    
    def add_summary_link_after_parent(self, title, link, parent_summary):
        self.add_summary_link_list_after_parent([{'title':title, 'link':link}], parent_summary)
    
    def add_summary_link_list_after_parent(self, summary_link_list, parent_summary):
        new_summary_list = []
        for summary in self.summary_list:
            new_summary_list.append(summary)
            if summary['id'] == parent_summary['id']:
                space = summary['space']
                for summary_link in summary_link_list:
                    title = summary_link['title']
                    link = summary_link['link']
                    new_summary_list.append(self.make_summary(f'  {space}', title, link))
        self.summary_list = new_summary_list

    def add_summary_link_after_brother(self, title, link, brother_summary):
        new_summary_list = []
        found = False
        finish = False
        brother_space = brother_summary['space']
        for summary in self.summary_list:
            if not found:
                if summary['id'] == brother_summary['id']:
                    found = True
            elif not finish:
                type = summary['type']
                if type == 'link':
                    if len(summary['space']) <= len(brother_space):
                        new_summary_list.append(self.make_summary(brother_space, title, link))
                        finish = True
            new_summary_list.append(summary)
        if found and not finish:
            new_summary_list.append(self.make_summary(brother_space, title, link))
        self.summary_list = new_summary_list

    def truncate_summary(self, parent_summary, truncate_num):
        new_summary_list = []
        truncate_list = []
        found = False
        finish = False
        summary_count = 0
        parent_space = parent_summary['space']
        for summary in self.summary_list:
            if not found:
                if summary['id'] == parent_summary['id']:
                    found = True
                new_summary_list.append(summary)
            elif not finish:
                if len(summary['space']) <= len(parent_space):
                    finish = True
                    new_summary_list.append(summary)
                else:
                    summary_count += 1
                    if summary_count <= truncate_num:
                        new_summary_list.append(summary)
                    else:
                        truncate_list.append(summary)
            else:
                new_summary_list.append(summary)
        self.summary_list = new_summary_list
        return truncate_list

    def to_markdown(self):
        lines = []
        for summary in self.summary_list:
            type = summary['type']
            if type == 'link':
                space = summary['space']
                title = summary['title']
                link = summary['link']
                lines.append(f'{space}* [{title}]({link})')
            elif type == 'line':
                lines.append(summary['line'])
        return '\n'.join(lines)

import lanying_redis
import logging
import time
import lanying_chatbot
from datetime import datetime
from datetime import date as datetime_date
from datetime import timedelta as datetime_timedelta
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
import yaml
import lanying_cdn
import lanying_cert
import lanying_slack
import lanying_google_analytics
from urllib.parse import urlparse,urlunparse
import lanying_baidu
import lanying_google
import lanying_oss
from github import Github

class TaskSetting:
    def __init__(self, app_id, name, note, chatbot_id, prompt, keywords, word_count_min, word_count_max, image_count, article_count, cycle_type, cycle_interval, file_list, deploy, title_reuse, site_id_list, target_dir, commit_type, target_summary_dir, embedding_condition, auto_deploy):
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
        self.embedding_condition = embedding_condition
        self.auto_deploy = auto_deploy

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
            'target_summary_dir': self.target_summary_dir,
            'embedding_condition': json.dumps(self.embedding_condition, ensure_ascii=False),
            'auto_deploy': self.auto_deploy
        }

class SiteSetting:
    def __init__(self, app_id, tenement_id, name, type, github_url, github_token, github_base_branch, github_base_dir, footer_note, lanying_link, title, copyright, canonical_link, meta_keywords, baidu_token, official_website_url, google_token, max_latest_num, language, commit_type, icp_number, hook_sentence_slogan, hook_sentence_image, github_hosting, collaborator):
        self.app_id = app_id
        self.tenement_id = tenement_id
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
        self.language = language
        self.commit_type = commit_type
        self.icp_number = icp_number
        self.hook_sentence_slogan = hook_sentence_slogan
        self.hook_sentence_image = hook_sentence_image
        self.github_hosting = github_hosting
        self.collaborator = collaborator

    def to_hmset_fields(self):
        return {
            'app_id': self.app_id,
            'tenement_id': self.tenement_id,
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
            'max_latest_num': self.max_latest_num,
            'language': self.language,
            'commit_type': self.commit_type,
            'icp_number': self.icp_number,
            'hook_sentence_slogan': self.hook_sentence_slogan,
            'hook_sentence_image': self.hook_sentence_image,
            'github_hosting': self.github_hosting,
            'collaborator': self.collaborator
        }

def handle_schedule(schedule_info):
    logging.info(f"grow_ai handle_schedule start | {schedule_info}")
    module = schedule_info['module']
    args = schedule_info['args']
    if module == 'lanying_grow_ai':
        logging.info(f"grow_ai handle_schedule run task| {schedule_info}")
        app_id = args['app_id']
        task_id = args['task_id']
        if is_deduct_failed(app_id):
            logging.info(f"handle_schedule skip deduct failed app_id:{app_id}task_id:{task_id}")
            return
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
    result = check_task_content_security(app_id, task_setting)
    if result['result'] == 'error':
        return result
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
    result = check_task_content_security(app_id, task_setting)
    if result['result'] == 'error':
        return result
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
            'success': True,
            'changed_chatbot_ids': []
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
                    task_info['site_cdn_token'] = site['site_cdn_token']
                if site and 'custom_site_url' in site and len(site['custom_site_url']) > 0:
                    task_info['custom_site_url'] = site['custom_site_url']
                if site:
                    for field in ['deploy_result', 'deploy_failed_reason', 'baidu_index_pages', 'baidu_index_domain', 'baidu_index_update_time', 'google_index_pages', 'google_index_domain', 'google_index_update_time']:
                        if field in site:
                            task_info[field] = site[field]
            task_list.append(task_info)
    return {
        'result': 'ok',
        'data':
            {
                'list': task_list
            }
    }
    
def get_task_id_list(app_id):
    redis = lanying_redis.get_redis_connection()
    return list(reversed(lanying_redis.redis_lrange(redis, get_task_list_key(app_id), 0, -1)))

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
            elif key in ['file_list', 'deploy', 'site_id_list', 'embedding_condition']:
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
        if 'auto_deploy' not in dto:
            dto['auto_deploy'] = 'on' if dto['site_id_list'] else 'off'
        if 'target_dir' not in dto:
            dto['target_dir'] = dto.get('deploy',{}).get('gitbook_target_dir', '/articles')
        if 'commit_type' not in dto:
            dto['commit_type'] = dto.get('deploy',{}).get('commit_type', 'branch')
        if 'target_summary_dir' not in dto:
            dto['target_summary_dir'] = ''
        if 'embedding_condition' not in dto:
            dto['embedding_condition'] = {}
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
    preview_id = task_run.get('preview_id', '')
    if preview_id and get_preview(app_id, preview_id) is not None:
        return {'result': 'error', 'message': 'task_run has preview'}
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

def is_deduct_failed(app_id):
    return lanying_config.get_app_config_boolean_from_redis(app_id, 'lanying_connector.new_deduct_failed', False)

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

def get_article_used(app_id, task_id):
    redis = lanying_redis.get_redis_connection()
    key = article_title_used_key(app_id, task_id)
    return lanying_redis.redis_hgetall(redis, key)

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
                        lines = readlines_auto(filename)
                        for line in lines:
                            if len(line) > 0 and len(line) < 1000 and not line.isspace():
                                keywords.append(line)
            except Exception as e:
                logging.exception(e)
    logging.info(f"parse_file_keywords finish | app_id:{app_id}, task_id:{task_id}, file_list:{file_list}, keyword count:{len(keywords)}")
    return keywords

def readlines_auto(path):
    encodings = (
        "utf-8-sig",   # UTF-8（含 BOM）
        "utf-8",
        "utf-16",      # UTF-16 LE / BE
        "gb18030",     # 覆盖 GBK / GB2312
        "big5",
    )

    for enc in encodings:
        try:
            with open(path, "r", encoding=enc) as f:
                return f.readlines()
        except UnicodeDecodeError:
            continue

    # 最后兜底：强行读（不推荐，但保证不炸）
    with open(path, "r", encoding="utf-8", errors="ignore") as f:
        return f.readlines()

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
    cycle_type = task_run.get('cycle_type', 'none')
    if len(keywords) == 0:
        if cycle_type == 'cycle':
            set_task_schedule(app_id, task_id, "off", 'article titles are exhausted')
        return {'result': 'error', 'message': 'article title not exist', 'retry': False}
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
    site_list = get_auto_deploy_site_list(app_id, task_id)
    if site_list:
        from lanying_tasks import grow_ai_deply_task_run
        update_task_run_field(app_id, task_run_id, "deploy_status", "pending")
        grow_ai_deply_task_run.apply_async(args = [app_id, task_run_id], countdown=5)
    return {'result': 'ok'}


def get_auto_deploy_site_list(app_id, task_id):
    task = get_task(app_id, task_id)
    if task is None:
        return []
    site_list = get_task_site_list(task)
    if task.get('auto_deploy', 'on' if site_list else 'off') != 'on':
        return []
    return site_list

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
    if task_run['deploy_status'] not in ["wait", "error", "success", "pending"]:
        return {'result': 'error', 'message': 'task_run deploy_status cannot deploy'}
    if 'zip_file' not in task_run:
        return {'result': 'error', 'message': 'zip file not exist'}
    task = get_task(app_id, task_run['task_id'])
    sites = get_task_site_list(task) if task else []
    if sites:
        site_id = str(sites[0]['site_id'])
        redis = lanying_redis.get_redis_connection()
        lock_key = f'lanying-connector-grow-ai-preview-site-lock:{app_id}:{site_id}'
        with redis.lock(lock_key, timeout=120):
            site = get_site(app_id, site_id)
            if site and (site.get('active_preview_id', '') or site.get('pending_preview_id', '')):
                discard_result = discard_site_previews_for_direct_publish(app_id, site)
                if discard_result['result'] == 'error':
                    return discard_result
            from lanying_tasks import grow_ai_deply_task_run
            grow_ai_deply_task_run.apply_async(args=[app_id, task_run_id])
    else:
        from lanying_tasks import grow_ai_deply_task_run
        grow_ai_deply_task_run.apply_async(args=[app_id, task_run_id])
    return {'result': 'ok', 'data':{
        'success': True
    }}


def discard_site_previews_for_direct_publish(app_id, site):
    preview_ids = []
    for field in ['pending_preview_id', 'active_preview_id']:
        preview_id = site.get(field, '')
        if preview_id and preview_id not in preview_ids:
            preview_ids.append(preview_id)
    previews = [get_preview(app_id, preview_id) for preview_id in preview_ids]
    previews = [preview for preview in previews if preview is not None]
    if not previews:
        update_site_field(app_id, site['site_id'], 'pending_preview_id', '')
        update_site_field(app_id, site['site_id'], 'active_preview_id', '')
        return {'result': 'ok', 'data': {'success': True}}

    redis = lanying_redis.get_redis_connection()
    for preview in previews:
        context = get_preview_github_context(preview)
        if context['result'] == 'error':
            return context
        if preview.get('status') == 'pr_open' and preview.get('pr_number'):
            response = requests.patch(
                f"{context['api_url']}/pulls/{preview['pr_number']}",
                headers=context['headers'], json={'state': 'closed'})
            if response.status_code != 200:
                return {'result': 'error', 'message': 'github fail to close PR'}
            redis.srem(preview_pr_set_key(), f"{app_id}:{preview['preview_id']}")

    clear_preview = next(
        (preview for preview in previews if preview['preview_id'] == site.get('active_preview_id', '')),
        previews[0])
    clear_result = dispatch_clear_preview(clear_preview)
    if clear_result['result'] == 'error':
        return clear_result

    update_site_field(app_id, site['site_id'], 'pending_preview_id', '')
    update_site_field(app_id, site['site_id'], 'active_preview_id', '')
    for preview in previews:
        update_preview_field(app_id, preview['preview_id'], 'status', 'clearing')
        if preview['preview_id'] != clear_preview['preview_id'] and preview.get('status') not in ['building', 'deploying']:
            cleanup_preview_without_site(app_id, preview['preview_id'])
    logging.info(f"direct publish discarded previews | app_id:{app_id}, site_id:{site['site_id']}, preview_ids:{preview_ids}")
    return {'result': 'ok', 'data': {'success': True}}

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

def del_content_meta_key(content, key):
    pattern = r'^{}: (.*)\n?'.format(key)
    return re.sub(pattern, '', content, 1, re.MULTILINE)

def do_deploy_task_run_internal(app_id, task_run_id, has_retry_times, preview_branch=None):
    logging.info(f"deploy task_run start | app_id:{app_id}, task_run_id:{task_run_id}, has_retry_times:{has_retry_times}")
    timestr = datetime.now().strftime('%Y%m%d%H%M%S')
    task_run = get_task_run(app_id, task_run_id)
    if task_run is None:
        return {'result': 'error', 'message': 'task_run not exist', 'retry': False}
    if task_run['status'] != 'success':
        if 'zip_file' not in task_run:
            return {'result': 'error', 'message': 'task_run status cannot deploy', 'retry': False}
    if 'zip_file' not in task_run:
        return {'result': 'error', 'message': 'zip file not exist', 'retry': False}
    task_id = task_run['task_id']
    task = get_task(app_id, task_id)
    if task is None:
        return {'result': 'error', 'message': 'task not exist', 'retry': False}
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
    if site['github_hosting'] == 'on' and github_url.startswith(f'https://github.com/{get_github_org()}/'):
        github_token = get_github_token()
    if len(github_token) == 0:
        return {'result': 'error', 'message': 'deploy token is bad', 'retry': False}
    redis_lock = lanying_redis.get_redis_connection()
    lock_key = f'lanying-connector-deploy-task-lock:{github_owner}/{github_repo}'
    lock_start_time = time.perf_counter()
    logging.info(f"start wait for lock: {lock_key}")
    with redis_lock.lock(lock_key, timeout=1200):
        elapsed = time.perf_counter() - lock_start_time
        logging.info(f"get lock {lock_key} after {elapsed:.3f} seconds")
        commit_type = site.get('commit_type', 'branch')
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
        if preview_branch:
            new_branch = preview_branch
        elif commit_type == 'pull_request':
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
            logging.info(f"failed to get current sha: {github_api_url}, code: {response.status_code}, text: {response.text}")
            try:
                response_json = response.json()
                if response_json['message'] == 'Bad credentials':
                    return {'result': 'error', 'message': 'github get branch info failed', 'retry': False}
            except Exception as e:
                pass
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
        if preview_branch or commit_type == 'pull_request':
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
            readme_content = f"# {target_summary_dir.capitalize()}"
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
            readme_content = f"# {os.path.join(target_summary_relative_dir, latest_title).capitalize()}"
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
                readme_content = f"# {os.path.join(target_summary_relative_dir, datestr).capitalize()}"
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
        if commit_type == 'pull_request' and not preview_branch:
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
            'pr_url': pr_url,
            'base_commit_sha': commit_sha,
            'commit_sha': new_commit_sha,
            'branch': new_branch
        }}


PREVIEW_DEPLOY_WORKFLOW = 'preview_sub_site.yml'
PREVIEW_CLEAR_WORKFLOW = 'clear_preview_sub_site.yml'
PREVIEW_CALLBACK_TTL = 2 * 60 * 60


def preview_key(app_id, preview_id):
    return f'lanying_connector:grow_ai:preview:{app_id}:{preview_id}'


def preview_callback_key(code):
    return f'lanying_connector:grow_ai:preview_callback:{code}'


def preview_pr_set_key():
    return 'lanying_connector:grow_ai:preview_pr_open'


def preview_publish_commit_key(repository, commit_sha):
    return f'lanying_connector:grow_ai:preview_publish_commit:{repository}:{commit_sha}'


def generate_preview_id():
    return uuid.uuid4().hex[:12]


def update_preview_field(app_id, preview_id, field, value):
    redis = lanying_redis.get_redis_connection()
    redis.hset(preview_key(app_id, preview_id), field, value)


def get_preview(app_id, preview_id):
    redis = lanying_redis.get_redis_connection()
    info = lanying_redis.redis_hgetall(redis, preview_key(app_id, preview_id))
    if not info or 'preview_id' not in info:
        return None
    dto = dict(info)
    for field in ['create_time', 'pr_number']:
        if field in dto:
            dto[field] = int(dto[field])
    site = get_site(app_id, dto['site_id'])
    if site:
        maybe_add_site_url(site)
        site_name = site.get('site_name', '')
        preview_site_name = f'preview-{site_name}'
        dto['url'] = make_site_full_url(preview_site_name)
        dto['cdn_token'] = calc_site_cdn_token(preview_site_name)
    dto['retryable'] = dto.get('status') == 'error' and dto.get('error') in ['preview deploy failed', 'preview workflow dispatch failed']
    return dto


def set_preview_callback(code, info):
    redis = lanying_redis.get_redis_connection()
    redis.setex(preview_callback_key(code), PREVIEW_CALLBACK_TTL, json.dumps(info, ensure_ascii=False))


def get_preview_callback(code):
    if not code:
        return None
    redis = lanying_redis.get_redis_connection()
    value = lanying_redis.redis_get(redis, preview_callback_key(code))
    return json.loads(value) if value else None


def delete_preview_callback(code):
    redis = lanying_redis.get_redis_connection()
    redis.delete(preview_callback_key(code))


def consume_preview_callback(code):
    if not code:
        return None
    redis = lanying_redis.get_redis_connection()
    lock_key = f'lanying-connector-grow-ai-preview-callback-lock:{code}'
    with redis.lock(lock_key, timeout=30):
        callback = get_preview_callback(code)
        if callback is not None:
            delete_preview_callback(code)
        return callback


def get_preview_github_context(preview):
    site = get_site(preview['app_id'], preview['site_id'])
    if site is None:
        return {'result': 'error', 'message': 'site not found'}
    parsed = parse_github_url(site.get('github_url', ''))
    if parsed['result'] == 'error':
        return parsed
    repository = f"{parsed['github_owner']}/{parsed['github_repo']}"
    if site.get('github_hosting') != 'on' or parsed['github_owner'] != get_github_org():
        return {'result': 'error', 'message': 'preview only supports github hosting site'}
    return {
        'result': 'ok',
        'site': site,
        'owner': parsed['github_owner'],
        'repo': parsed['github_repo'],
        'repository': repository,
        'api_url': f'https://api.github.com/repos/{repository}',
        'headers': {
            'Authorization': f'token {get_github_token()}',
            'Accept': 'application/vnd.github.v3+json'
        }
    }


def task_run_preview(app_id, task_run_id):
    logging.info(f'task_run_preview start | app_id:{app_id}, task_run_id:{task_run_id}')
    task_run = get_task_run(app_id, task_run_id)
    if task_run is None or 'zip_file' not in task_run:
        return {'result': 'error', 'message': 'task_run status cannot preview'}
    task = get_task(app_id, task_run['task_id'])
    if task is None:
        return {'result': 'error', 'message': 'task not exist'}
    sites = get_task_site_list(task)
    if not sites:
        return {'result': 'error', 'message': 'no site to preview'}
    site = sites[0]
    if site.get('github_hosting') != 'on':
        return {'result': 'error', 'message': 'preview only supports github hosting site'}
    preview_id = generate_preview_id()
    site_id = str(site['site_id'])
    branch_name = f'growai-preview-{site_id}-{preview_id}'
    now = int(time.time())
    redis = lanying_redis.get_redis_connection()
    lock_key = f'lanying-connector-grow-ai-preview-site-lock:{app_id}:{site_id}'
    with redis.lock(lock_key, timeout=120):
        locked_site = get_site(app_id, site_id)
        old_pending_id = locked_site.get('pending_preview_id', '') if locked_site else ''
        redis.hmset(preview_key(app_id, preview_id), {
            'preview_id': preview_id,
            'app_id': app_id,
            'site_id': site_id,
            'task_run_id': task_run_id,
            'branch_name': branch_name,
            'status': 'building',
            'error': '',
            'create_time': now
        })
        update_site_field(app_id, site_id, 'pending_preview_id', preview_id)
        update_task_run_field(app_id, task_run_id, 'preview_id', preview_id)
        if old_pending_id and old_pending_id != preview_id:
            cancel_pending_preview(app_id, old_pending_id)
    from lanying_tasks import grow_ai_preview_task
    grow_ai_preview_task.apply_async(args=[app_id, preview_id])
    logging.info(f'task_run_preview scheduled | app_id:{app_id}, task_run_id:{task_run_id}, preview_id:{preview_id}, branch:{branch_name}')
    return {'result': 'ok', 'data': {'preview_id': preview_id}}


def do_preview_task(app_id, preview_id):
    logging.info(f'do_preview_task start | app_id:{app_id}, preview_id:{preview_id}')
    preview = get_preview(app_id, preview_id)
    if preview is not None and preview.get('status') == 'clearing':
        cleanup_preview_without_site(app_id, preview_id)
        return {'result': 'ok', 'data': {'cancelled': True}}
    if preview is None or preview.get('status') != 'building':
        return {'result': 'error', 'message': 'preview cannot build'}
    result = do_deploy_task_run_internal(app_id, preview['task_run_id'], False, preview['branch_name'])
    if result['result'] == 'error':
        update_preview_field(app_id, preview_id, 'status', 'error')
        update_preview_field(app_id, preview_id, 'error', result['message'])
        return result
    data = result['data']
    update_preview_field(app_id, preview_id, 'base_commit_sha', data['base_commit_sha'])
    update_preview_field(app_id, preview_id, 'preview_commit_sha', data['commit_sha'])
    preview = get_preview(app_id, preview_id)
    if preview is None or preview.get('status') == 'clearing':
        cleanup_preview_without_site(app_id, preview_id)
        return {'result': 'ok', 'data': {'cancelled': True}}
    dispatch_result = dispatch_preview_workflow(preview)
    if dispatch_result['result'] == 'error':
        update_preview_field(app_id, preview_id, 'status', 'error')
        update_preview_field(app_id, preview_id, 'error', dispatch_result['message'])
    return dispatch_result


def dispatch_preview_workflow(preview):
    context = get_preview_github_context(preview)
    if context['result'] == 'error':
        return context
    site = context['site']
    maybe_add_site_url(site)
    site_name = site.get('site_name', '')
    if not re.fullmatch(r'[A-Za-z0-9-]+', site_name):
        return {'result': 'error', 'message': 'site_name is bad'}
    check_code = uuid.uuid4().hex
    callback_code = uuid.uuid4().hex
    callback_info = {
        'app_id': preview['app_id'], 'preview_id': preview['preview_id']
    }
    set_preview_callback(check_code, {
        **callback_info, 'type': 'deploy_check'
    })
    set_preview_callback(callback_code, {
        **callback_info, 'type': 'deploy_finish'
    })
    connector_server = lanying_utils.get_internet_connector_server()
    workflow_url = f'https://api.github.com/repos/maxim-top/im.gitbook/actions/workflows/{PREVIEW_DEPLOY_WORKFLOW}/dispatches'
    payload = {
        'ref': 'master',
        'inputs': {
            'repository': context['repository'],
            'commit_sha': preview['preview_commit_sha'],
            'site_name': site_name,
            'check_url': f'{connector_server}/grow_ai/check_preview_deploy?code={check_code}',
            'callback_url': f'{connector_server}/grow_ai/preview_deploy_finish?code={callback_code}'
        }
    }
    response = requests.post(workflow_url, headers=get_grow_ai_workflow_headers(), json=payload)
    if response.status_code != 204:
        logging.info(f'preview workflow dispatch failed | code:{response.status_code}, text:{response.text}')
        delete_preview_callback(check_code)
        delete_preview_callback(callback_code)
        return {'result': 'error', 'message': 'preview workflow dispatch failed'}
    return {'result': 'ok', 'data': {'success': True}}


def preview_retry(app_id, preview_id):
    preview = get_preview(app_id, preview_id)
    if preview is None or not preview.get('retryable') or not preview.get('preview_commit_sha'):
        return {'result': 'error', 'message': 'preview cannot retry'}
    redis = lanying_redis.get_redis_connection()
    lock_key = f"lanying-connector-grow-ai-preview-site-lock:{app_id}:{preview['site_id']}"
    with redis.lock(lock_key, timeout=120):
        site = get_site(app_id, preview['site_id'])
        if site is None:
            return {'result': 'error', 'message': 'site not found'}
        if site.get('pending_preview_id', '') not in ['', preview_id]:
            return {'result': 'error', 'message': 'another preview is building'}
        update_site_field(app_id, preview['site_id'], 'pending_preview_id', preview_id)
        update_preview_field(app_id, preview_id, 'status', 'building')
        update_preview_field(app_id, preview_id, 'error', '')
    result = dispatch_preview_workflow(get_preview(app_id, preview_id))
    if result['result'] == 'error':
        update_preview_field(app_id, preview_id, 'status', 'error')
        update_preview_field(app_id, preview_id, 'error', result['message'])
    return result


def check_preview_deploy(code, release_size):
    callback = consume_preview_callback(code)
    if callback is None or callback.get('type') != 'deploy_check':
        return {'result': 'error', 'message': 'code not found'}
    preview = get_preview(callback['app_id'], callback['preview_id'])
    if preview is None:
        return {'result': 'error', 'message': 'preview not found'}
    redis = lanying_redis.get_redis_connection()
    lock_key = f"lanying-connector-grow-ai-preview-site-lock:{preview['app_id']}:{preview['site_id']}"
    with redis.lock(lock_key, timeout=120):
        site = get_site(preview['app_id'], preview['site_id'])
        current_preview = get_preview(preview['app_id'], preview['preview_id'])
        if site is None or current_preview is None or current_preview.get('status') == 'clearing' or site.get('pending_preview_id', '') != preview['preview_id']:
            return {'result': 'error', 'message': 'preview superseded'}
        update_preview_field(preview['app_id'], preview['preview_id'], 'status', 'deploying')
        update_preview_field(preview['app_id'], preview['preview_id'], 'release_size', int(release_size))
    return {'result': 'ok', 'data': {'success': True}}


def preview_deploy_finish(code, status):
    logging.info(f'preview_deploy_finish | status:{status}')
    callback = consume_preview_callback(code)
    if callback is None or callback.get('type') != 'deploy_finish':
        return {'result': 'error', 'message': 'code not found'}
    app_id = callback['app_id']
    preview_id = callback['preview_id']
    preview = get_preview(app_id, preview_id)
    if preview is None:
        return {'result': 'error', 'message': 'preview not found'}
    if status != 'ok':
        if preview.get('status') == 'clearing':
            cleanup_preview_without_site(app_id, preview_id)
            return {'result': 'ok', 'data': {'success': True}}
        redis = lanying_redis.get_redis_connection()
        lock_key = f"lanying-connector-grow-ai-preview-site-lock:{app_id}:{preview['site_id']}"
        with redis.lock(lock_key, timeout=120):
            update_preview_field(app_id, preview_id, 'status', 'error')
            update_preview_field(app_id, preview_id, 'error', 'preview deploy failed')
            site = get_site(app_id, preview['site_id'])
            if site and site.get('pending_preview_id', '') == preview_id:
                update_site_field(app_id, preview['site_id'], 'pending_preview_id', '')
        restore_active_preview(preview)
        return {'result': 'ok', 'data': {'success': False}}
    if preview.get('status') == 'clearing':
        cleanup_preview_without_site(app_id, preview_id)
        return {'result': 'ok', 'data': {'success': True}}
    redis = lanying_redis.get_redis_connection()
    lock_key = f"lanying-connector-grow-ai-preview-site-lock:{app_id}:{preview['site_id']}"
    with redis.lock(lock_key, timeout=120):
        site = get_site(app_id, preview['site_id'])
        if site is None or site.get('pending_preview_id', '') != preview_id:
            update_preview_field(app_id, preview_id, 'status', 'error')
            update_preview_field(app_id, preview_id, 'error', 'preview superseded')
            return {'result': 'error', 'message': 'preview superseded'}
        old_preview_id = site.get('active_preview_id', '')
        update_site_field(app_id, preview['site_id'], 'active_preview_id', preview_id)
        update_site_field(app_id, preview['site_id'], 'pending_preview_id', '')
        restored_status = preview.get('restore_status', '')
        update_preview_field(app_id, preview_id, 'status', restored_status or 'ready')
        if restored_status:
            redis.hdel(preview_key(app_id, preview_id), 'restore_status')
        if old_preview_id and old_preview_id != preview_id:
            old_preview = get_preview(app_id, old_preview_id)
            if old_preview and old_preview.get('status') not in ['publishing', 'pr_open']:
                cleanup_preview_without_site(app_id, old_preview_id)
    return {'result': 'ok', 'data': {'success': True}}


def restore_active_preview(failed_preview):
    site = get_site(failed_preview['app_id'], failed_preview['site_id'])
    if site is None:
        return
    active_preview_id = site.get('active_preview_id', '')
    if not active_preview_id or active_preview_id == failed_preview['preview_id']:
        return
    active_preview = get_preview(failed_preview['app_id'], active_preview_id)
    if active_preview is None or not active_preview.get('preview_commit_sha'):
        return
    update_site_field(failed_preview['app_id'], failed_preview['site_id'], 'pending_preview_id', active_preview_id)
    update_preview_field(failed_preview['app_id'], active_preview_id, 'restore_status', active_preview.get('status', 'ready'))
    update_preview_field(failed_preview['app_id'], active_preview_id, 'status', 'building')
    result = dispatch_preview_workflow(get_preview(failed_preview['app_id'], active_preview_id))
    if result['result'] == 'error':
        update_site_field(failed_preview['app_id'], failed_preview['site_id'], 'pending_preview_id', '')
        update_preview_field(failed_preview['app_id'], active_preview_id, 'status', active_preview.get('status', 'ready'))
        redis = lanying_redis.get_redis_connection()
        redis.hdel(preview_key(failed_preview['app_id'], active_preview_id), 'restore_status')
        logging.error(f"restore active preview failed | app_id:{failed_preview['app_id']}, preview_id:{active_preview_id}, result:{result}")


def preview_publish(app_id, preview_id):
    logging.info(f'preview_publish start | app_id:{app_id}, preview_id:{preview_id}')
    preview = get_preview(app_id, preview_id)
    if preview is None or preview.get('status') != 'ready':
        return {'result': 'error', 'message': 'preview cannot publish'}
    context = get_preview_github_context(preview)
    if context['result'] == 'error':
        return context
    site = context['site']
    base_branch = site.get('github_base_branch', 'master')
    redis = lanying_redis.get_redis_connection()
    lock_key = f"lanying-connector-deploy-task-lock:{context['repository']}"
    with redis.lock(lock_key, timeout=1200):
        base_response = requests.get(f"{context['api_url']}/git/refs/heads/{base_branch}", headers=context['headers'])
        branch_response = requests.get(f"{context['api_url']}/git/refs/heads/{preview['branch_name']}", headers=context['headers'])
        if base_response.status_code != 200 or branch_response.status_code != 200:
            return {'result': 'error', 'message': 'github get branch info failed'}
        if base_response.json()['object']['sha'] != preview.get('base_commit_sha'):
            update_preview_field(app_id, preview_id, 'status', 'error')
            update_preview_field(app_id, preview_id, 'error', 'base branch changed')
            return {'result': 'error', 'message': 'base branch changed'}
        if branch_response.json()['object']['sha'] != preview.get('preview_commit_sha'):
            return {'result': 'error', 'message': 'preview branch changed'}
        if site.get('commit_type', 'branch') == 'pull_request':
            payload = {
                'title': f"Grow AI preview: {preview['task_run_id']}",
                'body': f"Grow AI preview: {preview_id}",
                'head': preview['branch_name'],
                'base': base_branch
            }
            response = requests.post(f"{context['api_url']}/pulls", headers=context['headers'], json=payload)
            if response.status_code != 201:
                return {'result': 'error', 'message': 'github fail to commit PR'}
            pr = response.json()
            update_preview_field(app_id, preview_id, 'status', 'pr_open')
            update_preview_field(app_id, preview_id, 'pr_url', pr.get('html_url', ''))
            update_preview_field(app_id, preview_id, 'pr_number', pr['number'])
            redis.sadd(preview_pr_set_key(), f'{app_id}:{preview_id}')
            return {'result': 'ok', 'data': {'pr_url': pr.get('html_url', '')}}
        response = requests.patch(
            f"{context['api_url']}/git/refs/heads/{base_branch}", headers=context['headers'],
            json={'sha': preview['preview_commit_sha'], 'force': False})
        if response.status_code != 200:
            return {'result': 'error', 'message': 'github fail to move commit'}
        update_preview_field(app_id, preview_id, 'status', 'publishing')
        update_preview_field(app_id, preview_id, 'publish_commit_sha', preview['preview_commit_sha'])
        redis.set(preview_publish_commit_key(context['repository'], preview['preview_commit_sha']), f'{app_id}:{preview_id}')
        return {'result': 'ok', 'data': {'success': True}}


def preview_discard(app_id, preview_id):
    logging.info(f'preview_discard start | app_id:{app_id}, preview_id:{preview_id}')
    preview = get_preview(app_id, preview_id)
    if preview is None:
        return {'result': 'ok', 'data': {'success': True}}
    context = get_preview_github_context(preview)
    redis = lanying_redis.get_redis_connection()
    lock_key = f"lanying-connector-grow-ai-preview-site-lock:{app_id}:{preview['site_id']}"
    with redis.lock(lock_key, timeout=120):
        site = get_site(app_id, preview['site_id'])
        is_active = site is not None and site.get('active_preview_id', '') == preview_id
        pending_preview_id = site.get('pending_preview_id', '') if site else ''
        if is_active and pending_preview_id and pending_preview_id != preview_id:
            return {'result': 'error', 'message': 'another preview is building'}
        if context['result'] == 'ok' and preview.get('status') == 'pr_open' and preview.get('pr_number'):
            close_response = requests.patch(f"{context['api_url']}/pulls/{preview['pr_number']}", headers=context['headers'], json={'state': 'closed'})
            if close_response.status_code != 200:
                return {'result': 'error', 'message': 'github fail to close PR'}
            redis.srem(preview_pr_set_key(), f'{app_id}:{preview_id}')
        if is_active:
            return dispatch_clear_preview(preview)
        if preview.get('status') in ['building', 'deploying']:
            cancel_pending_preview(app_id, preview_id)
            active_id = site.get('active_preview_id', '') if site else ''
            active_preview = get_preview(app_id, active_id) if active_id else None
            if preview.get('status') == 'deploying':
                if active_preview:
                    restore_active_preview(preview)
                else:
                    dispatch_clear_preview(preview)
            return {'result': 'ok', 'data': {'success': True}}
    cleanup_preview_without_site(app_id, preview_id)
    return {'result': 'ok', 'data': {'success': True}}


def dispatch_clear_preview(preview):
    context = get_preview_github_context(preview)
    if context['result'] == 'error':
        return context
    site = context['site']
    maybe_add_site_url(site)
    code = uuid.uuid4().hex
    set_preview_callback(code, {'type': 'clear', 'app_id': preview['app_id'], 'preview_id': preview['preview_id']})
    connector_server = lanying_utils.get_internet_connector_server()
    workflow_url = f'https://api.github.com/repos/maxim-top/im.gitbook/actions/workflows/{PREVIEW_CLEAR_WORKFLOW}/dispatches'
    response = requests.post(workflow_url, headers=get_grow_ai_workflow_headers(), json={
        'ref': 'master',
        'inputs': {
            'site_name': site['site_name'],
            'callback_url': f'{connector_server}/grow_ai/preview_clear_finish?code={code}'
        }
    })
    if response.status_code != 204:
        delete_preview_callback(code)
        return {'result': 'error', 'message': 'preview clear workflow dispatch failed'}
    update_preview_field(preview['app_id'], preview['preview_id'], 'status', 'clearing')
    return {'result': 'ok', 'data': {'success': True}}


def preview_clear_finish(code, status):
    logging.info(f'preview_clear_finish | status:{status}')
    callback = consume_preview_callback(code)
    if callback is None or callback.get('type') != 'clear':
        return {'result': 'error', 'message': 'code not found'}
    if status != 'ok':
        update_preview_field(callback['app_id'], callback['preview_id'], 'status', 'error')
        update_preview_field(callback['app_id'], callback['preview_id'], 'error', 'preview clear failed')
        return {'result': 'ok', 'data': {'success': False}}
    if not cleanup_preview_without_site(callback['app_id'], callback['preview_id']):
        return {'result': 'error', 'message': 'preview branch cleanup failed'}
    return {'result': 'ok', 'data': {'success': True}}


def cleanup_preview_without_site(app_id, preview_id):
    preview = get_preview(app_id, preview_id)
    if preview is None:
        return True
    context = get_preview_github_context(preview)
    if context['result'] == 'error' and preview.get('branch_name'):
        update_preview_field(app_id, preview_id, 'status', 'error')
        update_preview_field(app_id, preview_id, 'error', context['message'])
        return False
    if context['result'] == 'ok' and preview.get('branch_name'):
        response = requests.get(f"{context['api_url']}/git/refs/heads/{preview['branch_name']}", headers=context['headers'])
        if response.status_code == 200:
            if preview.get('preview_commit_sha') and response.json()['object']['sha'] != preview.get('preview_commit_sha'):
                update_preview_field(app_id, preview_id, 'status', 'error')
                update_preview_field(app_id, preview_id, 'error', 'preview branch changed')
                return False
            delete_response = requests.delete(f"{context['api_url']}/git/refs/heads/{preview['branch_name']}", headers=context['headers'])
            if delete_response.status_code != 204:
                update_preview_field(app_id, preview_id, 'status', 'error')
                update_preview_field(app_id, preview_id, 'error', 'preview branch cleanup failed')
                return False
        elif response.status_code != 404:
            update_preview_field(app_id, preview_id, 'status', 'error')
            update_preview_field(app_id, preview_id, 'error', 'preview branch cleanup failed')
            return False
    redis = lanying_redis.get_redis_connection()
    site = get_site(app_id, preview['site_id'])
    if site:
        if site.get('active_preview_id', '') == preview_id:
            update_site_field(app_id, preview['site_id'], 'active_preview_id', '')
        if site.get('pending_preview_id', '') == preview_id:
            update_site_field(app_id, preview['site_id'], 'pending_preview_id', '')
    task_run = get_task_run(app_id, preview['task_run_id'])
    if task_run and task_run.get('preview_id') == preview_id:
        update_task_run_field(app_id, preview['task_run_id'], 'preview_id', '')
    redis.srem(preview_pr_set_key(), f'{app_id}:{preview_id}')
    if context['result'] == 'ok' and preview.get('publish_commit_sha'):
        redis.delete(preview_publish_commit_key(context['repository'], preview['publish_commit_sha']))
    redis.delete(preview_key(app_id, preview_id))
    return True


def cancel_pending_preview(app_id, preview_id):
    preview = get_preview(app_id, preview_id)
    if preview is None:
        return
    update_preview_field(app_id, preview_id, 'status', 'clearing')
    site = get_site(app_id, preview['site_id'])
    if site and site.get('pending_preview_id', '') == preview_id:
        update_site_field(app_id, preview['site_id'], 'pending_preview_id', '')


def reconcile_preview_pull_requests():
    redis = lanying_redis.get_redis_connection()
    for item in list(redis.smembers(preview_pr_set_key())):
        if isinstance(item, bytes):
            item = item.decode()
        app_id, preview_id = item.split(':', 1)
        preview = get_preview(app_id, preview_id)
        if preview is None:
            redis.srem(preview_pr_set_key(), item)
            continue
        context = get_preview_github_context(preview)
        if context['result'] == 'error':
            continue
        response = requests.get(f"{context['api_url']}/pulls/{preview['pr_number']}", headers=context['headers'])
        if response.status_code != 200:
            continue
        pr = response.json()
        if pr.get('merged'):
            commit_sha = pr.get('merge_commit_sha', '')
            merge_tree_sha = get_github_commit_tree_sha(context, commit_sha)
            preview_tree_sha = get_github_commit_tree_sha(context, preview.get('preview_commit_sha', ''))
            if not merge_tree_sha or merge_tree_sha != preview_tree_sha:
                update_preview_field(app_id, preview_id, 'status', 'error')
                update_preview_field(app_id, preview_id, 'error', 'published content changed')
                redis.srem(preview_pr_set_key(), item)
                logging.info(f'preview PR content changed | app_id:{app_id}, preview_id:{preview_id}, merge_commit:{commit_sha}')
                continue
            update_preview_field(app_id, preview_id, 'status', 'publishing')
            update_preview_field(app_id, preview_id, 'publish_commit_sha', commit_sha)
            redis.set(preview_publish_commit_key(context['repository'], commit_sha), f'{app_id}:{preview_id}')
            redis.srem(preview_pr_set_key(), item)
        elif pr.get('state') == 'closed':
            site = context['site']
            redis.srem(preview_pr_set_key(), item)
            if site.get('active_preview_id', '') == preview_id:
                update_preview_field(app_id, preview_id, 'status', 'ready')
                update_preview_field(app_id, preview_id, 'pr_url', '')
            else:
                cleanup_preview_without_site(app_id, preview_id)


def get_github_commit_tree_sha(context, commit_sha):
    if not re.fullmatch(r'[0-9a-fA-F]{40}', commit_sha or ''):
        return ''
    response = requests.get(f"{context['api_url']}/git/commits/{commit_sha}", headers=context['headers'])
    if response.status_code != 200:
        return ''
    return response.json().get('tree', {}).get('sha', '')


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

def generate_article(app_id, task_id, task_run_id, keyword, from_user_id, chatbot_user_id, text_prompt, word_count_min, word_count_max, embedding_condition):
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
                    "embedding_condition": embedding_condition,
                    'reset_prompt': True
                }
            }
        else:
            prompt_ext = {
                'ai': {
                    "history_msg_size_max": 4096,
                    "max_tokens": 4096,
                    "embedding_condition": embedding_condition
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
    metadata, now_article_text = parse_content_metadata(now_article_text)
    article_url_prefix = find_metadata_key(metadata, 'url', '')
    return {'result': 'ok', 'article_text': now_article_text, 'article_url_prefix': article_url_prefix,  "message_quota_usage": message_quota_usage}

def find_title_from_content(content):
    match = re.search(r'^(#|title:) (.*)', content, re.MULTILINE)
    if match:
        return match.group(2).strip('" ')
    else:
        return '无标题'

def find_metadata_key(metadata, key, default):
    pattern = r'<{}>(.*)</{}>'.format(key, key)
    match = re.search(pattern, metadata, re.MULTILINE)
    if match:
        return match.group(1).strip('" ')
    else:
        return default

def parse_content_metadata(content):
    match = re.search(r'<metadata>.*?</metadata>', content, flags=re.DOTALL)
    if match:
        metadata = match.group(0)
    else:
        metadata = ''
    logging.info(f"got metadata: {metadata}")
    content = re.sub(r'<metadata>.*?</metadata>\n*', '', content, flags=re.DOTALL)
    description = collect_description_from_content(content)
    keywords = find_metadata_key(metadata, 'keywords', '')
    extra_keywords = find_metadata_key(metadata, 'extra_keywords', '')
    if extra_keywords != '':
        if keywords == '':
            keywords = extra_keywords
        else:
            keywords = f'{keywords}, {extra_keywords}'
    description_escaped = yaml.dump(description, default_style='"', allow_unicode=True).strip()
    keywords_escaped = yaml.dump(keywords, default_style='"', allow_unicode=True).strip()
    header = f'---\ndescription: {description_escaped}\nkeywords: {keywords_escaped}\n---\n'
    content = f'{header}{content}\n'
    return metadata, content

def collect_description_from_content(content):
    for line in content.split('\n'):
        if '# ' not in line and len(line) > 10:
            if len(line) > 1000:
                line = line[:1000]
            return line
    return content[:1000]

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
    embedding_condition = task.get('embedding_condition',{})
    from_user_id = task_run['user_id']
    site_list = get_task_site_list(task)
    site_language = 'zh-hans'
    if len(site_list) > 0:
        meta_keywords = site_list[0].get('meta_keywords', '')
        site_language = site_list[0].get('language', 'zh-hans')
        if meta_keywords != '':
            meta_keywords_prompt_1 = '<extra_keywords></extra_keywords>'
            meta_keywords_prompt_2 = f'5. extra_keywords: 搜索引擎SEO额外关键词，请从提供的可能的关键词列表中选出最多 2 个最适合作为本文关键词且不在[3. keywords]里的关键词，关键词列表可能为空或不足2个， 可能的关键词列表为：{meta_keywords}。\n'
        else:
            meta_keywords_prompt_1 = ''
            meta_keywords_prompt_2 = ''
    else:
        meta_keywords_prompt_1 = ''
        meta_keywords_prompt_2 = ''
    logging.info(f"use site language:{site_language}")
    action_language = ''
    if site_language == 'en':
        action_language = '英文'
    task_prompt = task['prompt']
    action_prompt = f"请生成一篇markdown格式的{action_language}文章，不要生成图片，第一行需要是文章标题：\n"
    word_prompt = f'字数范围 {word_count_min} - {word_count_max} 字\n'
    image_placeholder_text = '[插图]'
    image_placeholder_prompt = f'需要包含有且只有 1 个的插图占位标记,用于之后放置图片, 使用 {image_placeholder_text} 表示, 注意此占位符需要独占一行，且不要有加粗等格式修饰；\n' if image_count > 0 else ''
    meta_prompt = (f'请在文章中输出一份文章的元数据，位于文章摘要或文章标题后，元数据只能输出一份，不要多次输出，元数据为XML格式，如：<metadata><title></title><keywords></keywords><url></url>{meta_keywords_prompt_1}</metadata>。字段要求如下：\n'
                   '1. title: 文章标题。\n'
                   '2. keywords: 搜索引擎SEO关键词, 最多 2 个关键词, 字符串类型，多个关键词请以英文逗号连接。\n'
                   f'3. url: 标题的英文URL路径表示,建议的URL需要根据标题生成，形式是三到五个英文单词，使用连字符而不是下划线，使用小写字母,请尽量不要多于5个英文单词，为减少单词数量可以适当考虑缩写。\n{meta_keywords_prompt_2}')
    subject_prompt = '' if task_prompt == '' else f'如果文章内容涉及以下公司或产品，可在适当的时机引用说明并推荐：{task_prompt}\n'
    keyword_prompt = f'文章标题必须为：{keyword}\n'
    if site_language == 'en':
        extra_prompt = f'注意：一定不要生成图片。一定要使用英文，不要使用中文。\n'
    else:
        extra_prompt = f'注意：一定不要生成图片, 如果未指定中英文等语言，请默认使用中文。\n'
    text_prompt = f'{action_prompt}{word_prompt}{image_placeholder_prompt}{meta_prompt}{keyword_prompt}{subject_prompt}{extra_prompt}'
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
        text_result = generate_article(app_id, task_id, task_run_id, keyword, from_user_id, chatbot_user_id, text_prompt, word_count_min, word_count_max, embedding_condition)
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
    task = get_task(app_id, task_id)
    sites = get_task_site_list(task) if task else []
    site_has_preview = any(site.get('active_preview_id', '') or site.get('pending_preview_id', '') for site in sites)
    for task_run_id in task_run_ids:
        task_run_info = get_task_run(app_id, task_run_id)
        if task_run_info:
            task_run_info['site_has_preview'] = site_has_preview
            preview_id = task_run_info.get('preview_id', '')
            if preview_id:
                preview = get_preview(app_id, preview_id)
                if preview:
                    task_run_info['preview'] = preview
            task_run_list.append(task_run_info)
    return {
        'result': 'ok',
        'data':{
            'list': task_run_list
        }
    }

def get_task_run_id_list(app_id, task_id):
    redis = lanying_redis.get_redis_connection()
    return list(reversed(lanying_redis.redis_lrange(redis, get_task_run_list_key(app_id, task_id), 0, -1)))

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
    owner_app_id = None
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
                    owner_app_id = app_id
    if owner_site_id is not None:
        preview_id = ''
        commit_sha = resolve_release_commit(repository, release)
        if commit_sha:
            redis = lanying_redis.get_redis_connection()
            mapping = lanying_redis.redis_get(redis, preview_publish_commit_key(repository, commit_sha))
            if not mapping:
                reconcile_preview_pull_requests()
                mapping = lanying_redis.redis_get(redis, preview_publish_commit_key(repository, commit_sha))
            if mapping:
                mapped_app_id, preview_id = mapping.split(':', 1)
                if mapped_app_id != owner_app_id:
                    preview_id = ''
        return start_deploy_github_action(owner_app_id, '', owner_site_id, github_owner, github_repo, release, preview_id)
    return {'result': 'error', 'message': 'deploy not found'}

def resolve_release_commit(repository, release):
    headers = {
        'Authorization': f'token {get_github_token()}',
        'Accept': 'application/vnd.github.v3+json'
    }
    response = requests.get(f'https://api.github.com/repos/{repository}/releases/tags/{release}', headers=headers)
    if response.status_code != 200:
        logging.info(f'resolve release commit failed | repository:{repository}, release:{release}, code:{response.status_code}')
        return ''
    target = response.json().get('target_commitish', '')
    if re.fullmatch(r'[0-9a-fA-F]{40}', target):
        return target
    response = requests.get(f'https://api.github.com/repos/{repository}/commits/{target}', headers=headers)
    return response.json().get('sha', '') if response.status_code == 200 else ''


def start_deploy_github_action(app_id, task_id, site_id, github_owner, github_repo, release, preview_id=''):
    logging.info(f"start_deploy_github_action start | app_id:{app_id}, task_id:{task_id}, site_id:{site_id}, github_owner:{github_owner}, github_repo:{github_repo}, release:{release}")
    deploy_repo_owner = 'maxim-top'
    deploy_repo_name = 'im.gitbook'
    deploy_workflow_id = 'deploy_sub_site.yml'
    site_name = get_github_site(github_owner, github_repo)
    cdn_url = make_site_full_url(site_name)
    site = get_site(app_id, site_id)
    if site is not None:
        custom_site_url = site.get('custom_site_url', '')
        if custom_site_url != '':
            cdn_url = custom_site_url
    deploy_code = f"{uuid.uuid4()}-{int(time.time()*1000000)}"
    set_deploy_code(deploy_code, {
        'app_id': app_id,
        'task_id': task_id,
        'site_id': site_id,
        'github_owner': github_owner,
        'github_repo': github_repo,
        'preview_id': preview_id
    })
    connector_server = lanying_utils.get_internet_connector_server()
    # 构建请求头和请求URL
    headers = get_grow_ai_workflow_headers()
    url = f'https://api.github.com/repos/{deploy_repo_owner}/{deploy_repo_name}/actions/workflows/{deploy_workflow_id}/dispatches'

    # 请求体内容
    data = {
        'ref': 'master',
        'inputs': {
            'book_url': f'https://github.com/{github_owner}/{github_repo}/releases/download/{release}/book.tar.gz',
            'oss_path': f'/{site_name}',
            'cdn_url': cdn_url,
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
    if deploy_result != 'ok':
        update_site_field(app_id, site_id, "deploy_result", "failed")
        update_site_field(app_id, site_id, "deploy_failed_reason", "deploy workflow failed")
        return {'result':'ok', 'data':{'success': False}}
    update_site_field(app_id, site_id, "deploy_result", "success")
    update_site_field(app_id, site_id, "deploy_failed_reason", "")
    preview_id = code_info.get('preview_id', '')
    if preview_id:
        preview = get_preview(app_id, preview_id)
        if preview:
            redis = lanying_redis.get_redis_connection()
            lock_key = f'lanying-connector-grow-ai-preview-site-lock:{app_id}:{site_id}'
            with redis.lock(lock_key, timeout=120):
                current_site = get_site(app_id, site_id)
                if current_site.get('active_preview_id', '') == preview_id:
                    pending_preview_id = current_site.get('pending_preview_id', '')
                    if pending_preview_id and pending_preview_id != preview_id:
                        update_preview_field(app_id, preview_id, 'status', 'published')
                    else:
                        clear_result = dispatch_clear_preview(preview)
                        if clear_result['result'] == 'error':
                            update_preview_field(app_id, preview_id, 'status', 'error')
                            update_preview_field(app_id, preview_id, 'error', clear_result['message'])
                else:
                    cleanup_preview_without_site(app_id, preview_id)
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
            return {'result': 'error', 'message': 'github_url is bad', 'retry': False}
        github_owner = fields[3]
        github_repo = fields[4]
        if github_repo.endswith(".git"):
            github_repo = github_repo[:-4]
        return {'result': 'ok', 'github_owner': github_owner, "github_repo": github_repo}
    elif github_url.startswith("git@github.com:"):
        fields = re.split("[:/]{1,}", github_url)
        if len(fields) < 3:
            return {'result': 'error', 'message': 'github_url is bad', 'retry': False}
        github_owner = fields[1]
        github_repo = fields[2]
        if github_repo.endswith(".git"):
            github_repo = github_repo[:-4]
        return {'result': 'ok', 'github_owner': github_owner, "github_repo": github_repo}
    return {'result': 'error', 'message': 'github_url is bad', 'retry': False}


def create_site(site_setting: SiteSetting):
    now = int(time.time())
    app_id = site_setting.app_id
    result = check_site_num_limit(app_id)
    if result['result'] == 'error':
        return result
    result = check_site_setting(site_setting)
    if result['result'] == 'error':
        return result
    site_id = generate_site_id()
    redis = lanying_redis.get_redis_connection()
    fields = site_setting.to_hmset_fields()
    fields['status'] = 'normal'
    fields['create_time'] = now
    fields['site_id'] = site_id
    logging.info(f"create site start | app_id:{app_id}, site_info:{fields}")
    redis.hmset(get_site_key(app_id, site_id), fields)
    redis.rpush(get_site_list_key(app_id), site_id)
    add_to_all_site_list(app_id, site_id)
    site_info = get_site(app_id, site_id)
    logging.info(f"create site finish | app_id:{app_id}, site_info:{site_info}")
    site_info = maybe_init_github_site_repo(app_id, site_info, site_info)
    maybe_invite_github_member(app_id, {}, site_info)
    maybe_register_github_site(app_id, site_info)
    maybe_init_analytics(app_id, site_id)
    site_info = get_site(app_id, site_id)
    maybe_sync_to_github(site_info, site_info)
    return {
        'result': 'ok',
        'data': {
            'site_id': site_id
        }
    }

def check_site_num_limit(app_id):
    count = get_site_count(app_id)
    if app_id == 'ddabwkppllo':
        return {'result': "ok"}
    if count >= 5:
        return {'result': 'error', 'message': 'site count limit exceeded'}
    return {'result': "ok"}

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
    if fields['github_hosting'] == 'on':
        for field in ['github_url', 'github_token', 'github_base_branch']:
            del fields[field]
    logging.info(f"configure site start | app_id:{app_id}, site_info:{fields}")
    redis.hmset(get_site_key(app_id, site_id), fields)
    new_site_info = get_site(app_id, site_id)
    new_site_info = maybe_init_github_site_repo(app_id, site_info, new_site_info)
    maybe_invite_github_member(app_id, site_info, new_site_info)
    maybe_register_github_site(app_id, new_site_info)
    maybe_init_analytics(app_id, site_id)
    new_site_info = get_site(app_id, site_id)
    maybe_sync_to_github(site_info, new_site_info)
    return {
        'result': 'ok',
        'data': {
            'success': True
        }
    }

def maybe_sync_to_github(old_site, site):
    try:
        executor.submit(sync_to_github, old_site, site)
    except Exception as e:
        pass

def sync_to_github(old_site, site):
    github_url = site.get('github_url', '')
    github_token = site.get('github_token', '')
    if site['github_hosting'] == 'on' and github_url.startswith(f'https://github.com/{get_github_org()}/'):
        github_token = get_github_token()
    result = parse_github_url(github_url)
    if result['result'] == 'error':
        return result
    if old_site.get('hook_sentence_image', '') != site.get('hook_sentence_image', '') and site.get('hook_sentence_image', '') != '':
        download_url = site.get('hook_sentence_image', '')
        try:
            upload_file_to_github(site, download_url, "Update hook sentence image from LanyingIM Console")
        except Exception as e:
            logging.error(f"fail to upload_file_to_github | download_url:{download_url}")
            logging.exception(e)
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

def upload_file_to_github(site, download_url, commit_message):
    github_url = site.get('github_url', '')
    github_token = site.get('github_token', '')
    if site['github_hosting'] == 'on' and github_url.startswith(f'https://github.com/{get_github_org()}/'):
        github_token = get_github_token()
    result = parse_github_url(github_url)
    if result['result'] == 'error':
        return result
    cdn_url = lanying_oss.get_cdn_url()
    if not download_url.startswith(cdn_url):
        return {
            'result': 'error',
            'message': 'bad download_url'
        }
    response = requests.get(download_url)
    if response.status_code != 200:
        logging.info(f"upload_file_to_github fail to download | {download_url}")
        return {
            'result': 'error',
            'message': 'file not exist'
        }
    file_content = response.content
    github_owner = result['github_owner']
    github_repo = result['github_repo']
    github_api_url = f"https://api.github.com/repos/{github_owner}/{github_repo}"
    base_branch = site.get('github_base_branch', 'master')
    headers = {
        "Authorization": f"token {github_token}",
        "Accept": "application/vnd.github.v3+json"
    }
    asset_file = make_hook_sentence_image(site, download_url)
    asset_url = f"{github_api_url}/contents/{asset_file}"
    content_base64 = base64.b64encode(file_content).decode("utf-8")

    payload = {
        "message": commit_message,
        "content": content_base64,
        "branch": base_branch
    }

    response = requests.put(asset_url, headers=headers, json=payload)
    if response.status_code != 200:
        logging.info(f"upload_file_to_github response | {response.content}")
        return {'result': 'error', 'message': 'github fail to commit'}
    return {'result': 'ok'}

def get_url_filename(url):
    path = urlparse(url).path
    filename = os.path.basename(path)
    return filename

def transform_site_to_book_json(site, book_json, github_owner, github_repo, base_branch):
    new_book_json = copy.deepcopy(book_json)
    for field in ['language','title', 'github_buttons', 'copyright', 'edit_link', 'logo_site_url', 'canonical_link', 'meta_keywords', 'baidu_token', 'footer_note', 'lanying_link', 'sitemap_hostname', 'google_token', 'hook_sentence_slogan', 'hook_sentence_image']:
        try:
            if field == 'title':
                title = site.get('title', '')
                if len(title) > 0:
                    new_book_json['title'] = title
            elif field == 'language':
                language = site.get('language', '')
                if len(language) > 0:
                    new_book_json['language'] = language
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
                    language = site.get('language', 'zh-hans')
                    if language == 'en':
                        official_site_link = f" | <a href='{site_url}' target='_blank' style='text-decoration:none!important;'>Official Website</a>"
                        site_map_link = f" | <a href='/sitemap.xml' style='text-decoration:none!important;' target='_blank'>Sitemap</a>"
                    else:
                        official_site_link = f" | <a href='{site_url}' target='_blank' style='text-decoration:none!important;'>官网</a>"
                        site_map_link = f" | <a href='/sitemap.xml' style='text-decoration:none!important;' target='_blank'>网站地图</a>"
                    icp_number = site.get('icp_number', '')
                    if icp_number != '':
                        icp_number_link = f" | <a href='https://beian.miit.gov.cn' target='_blank' style='text-decoration:none!important;'>{icp_number}</a>"
                    else:
                        icp_number_link = ''
                    new_book_json['pluginsConfig']['tbfed-pagefooter']['copyright'] = f"{copyright}{icp_number_link}{official_site_link}{site_map_link}"
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
                    site['canonical_link'] = canonical_link
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
            elif field == 'hook_sentence_slogan':
                hook_sentence_slogan = site.get('hook_sentence_slogan', '')
                old_hook_sentence_slogan = new_book_json['pluginsConfig']['lanying-grow-ai'].get('hook_sentence_slogan', '')
                if hook_sentence_slogan != old_hook_sentence_slogan:
                    new_book_json['pluginsConfig']['lanying-grow-ai']['hook_sentence_slogan'] = hook_sentence_slogan
            elif field == 'hook_sentence_image':
                hook_sentence_image = site.get('hook_sentence_image', '')
                old_hook_sentence_image = new_book_json['pluginsConfig']['lanying-grow-ai'].get('hook_sentence_image', '')
                if hook_sentence_image != '':
                    asset_file = make_hook_sentence_image_relative(site, hook_sentence_image)
                    new_book_json['pluginsConfig']['lanying-grow-ai']['hook_sentence_image'] = asset_file
                elif hook_sentence_image != old_hook_sentence_image:
                    new_book_json['pluginsConfig']['lanying-grow-ai']['hook_sentence_image'] = ''
        except Exception as e:
            pass
    logging.info(f"transform_site_to_book_json | site:{site}, book_json:{book_json}, new_book_json:{new_book_json}")
    return new_book_json

def make_hook_sentence_image(site, download_url):
    filename = get_url_filename(download_url)
    base_dir = site.get('github_base_dir', '/').strip("/")
    return os.path.join(base_dir, "assets", "hook-sentence", filename)

def make_hook_sentence_image_relative(site, download_url):
    filename = get_url_filename(download_url)
    return os.path.join("assets", "hook-sentence", filename)

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

def get_site_count(app_id):
    redis = lanying_redis.get_redis_connection()
    return redis.llen(get_site_list_key(app_id))

def maybe_add_site_url(site_info):
    if site_info['type'] == 'gitbook':
        github_url = site_info['github_url']
        result = parse_github_url(github_url)
        if result['result'] == 'ok':
            github_owner = result['github_owner']
            github_repo = result['github_repo']
            site_name = get_github_site(github_owner, github_repo)
            site_url = make_site_full_url(site_name)
            site_info['site_name'] = site_name
            site_info['site_url'] = site_url
            cdn_token = calc_site_cdn_token(site_name)
            site_info['site_cdn_token'] = cdn_token

def calc_site_cdn_token(site_name):
    secret_key = os.getenv('LANYING_CONNECTOR_GROW_AI_CDN_SECRET')
    return lanying_utils.md5hex(f"{secret_key}{site_name}{secret_key}")

def get_site(app_id, site_id):
    redis = lanying_redis.get_redis_connection()
    key = get_site_key(app_id, site_id)
    info = lanying_redis.redis_hgetall(redis, key)
    if "create_time" in info:
        dto = {}
        for key,value in info.items():
            if key in ['create_time', 'update_time', 'website_storage', 'max_latest_num', 'baidu_index_pages', 'google_index_pages']:
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
        if 'language' not in dto:
            dto['language'] = 'zh-hans'
        if 'commit_type' not in dto:
            dto['commit_type'] = 'branch'
        if 'domain_id' not in dto:
            dto['domain_id'] = ''
        if 'icp_number' not in dto:
            dto['icp_number'] = ''
        if 'tenement_id' not in dto:
            dto['tenement_id'] = ''
        if 'baidu_index_pages' not in dto:
            dto['baidu_index_pages'] = 0
        if 'baidu_index_domain' not in dto:
            dto['baidu_index_domain'] = ''
        if 'baidu_index_update_time' not in dto:
            dto['baidu_index_update_time'] = ''
        if 'google_index_pages' not in dto:
            dto['google_index_pages'] = 0
        if 'google_index_domain' not in dto:
            dto['google_index_domain'] = ''
        if 'google_index_update_time' not in dto:
            dto['google_index_update_time'] = ''
        if 'hook_sentence_slogan' not in dto:
            dto['hook_sentence_slogan'] = ''
        if 'hook_sentence_image' not in dto:
            dto['hook_sentence_image'] = ''
        if 'github_hosting' not in dto:
            dto['github_hosting'] = 'off'
        if 'collaborator' not in dto:
            dto['collaborator'] = ''
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

def get_all_site_list_key():
    return 'lanying_connector:grow_ai:all_site_list'

def add_to_all_site_list(app_id, site_id):
    redis = lanying_redis.get_redis_connection()
    key = get_all_site_list_key()
    redis.hset(key, site_id, app_id)

def get_all_site_list():
    redis = lanying_redis.get_redis_connection()
    key = get_all_site_list_key()
    return lanying_redis.redis_hgetall(redis, key)

def get_all_site_detail_list():
    dtos = []
    for site_id, app_id in get_all_site_list().items():
        site = get_site(app_id, site_id)
        if site:
            dtos.append(site)
    dtos = sorted(dtos, key=lambda x: int(x['site_id']))
    return dtos

def init_all_site_list():
    redis = lanying_redis.get_redis_connection()
    keys = lanying_redis.redis_keys(redis, "lanying_connector:grow_ai:site:*")
    for key in keys:
        fields = key.split(':')
        if len(fields) == 5:
            add_to_all_site_list(fields[3], fields[4])

def site_statistics_fields():
    return ['activeUsers','newUsers','totalUsers','screenPageViews']

def update_site_statistics(app_id, site_id):
    site = get_site(app_id, site_id)
    if site:
        if 'google_analytics_property_name' in site:
            property_name = site['google_analytics_property_name']
            try:
                today = datetime_date.today()
                one_week_ago = today - datetime_timedelta(7)
                fields = site_statistics_fields()
                logging.info(f"update_site_statistics start | app_id: {app_id},site_id: {site_id}")
                result = lanying_google_analytics.get_report(property_name, one_week_ago, today, fields)
                logging.info(f"update_site_statistics result | app_id: {app_id},site_id: {site_id}, result:{result}")
                if result['result'] == 'ok':
                    data_list = result['data']['list']
                    redis = lanying_redis.get_redis_connection()
                    for data in data_list:
                        date = format_statistics_date(data['date'])
                        for category,value in data.items():
                            if category != 'date':
                                statistic_key = site_statistics_key(app_id, site_id, category)
                                redis.hset(statistic_key, date, value)
                    update_site_acc_statistics(app_id, site_id)
                    return {'result': 'ok'}
            except Exception as e:
                logging.exception(e)
    return {'result': 'error', 'message': 'not_updated'}

def get_site_all_statistics(app_id, site_id):
    redis = lanying_redis.get_redis_connection()
    fields = site_statistics_fields()
    dto = {}
    for field in fields:
        statistic_key = site_statistics_key(app_id, site_id, field)
        dto[field] = lanying_redis.redis_hgetall(redis, statistic_key)
    return dto

def get_site_statistics(app_id, site_id, start_date, end_date, targets):
    start_date = datetime.strptime(start_date, '%Y-%m-%d')
    end_date = datetime.strptime(end_date, '%Y-%m-%d')
    redis = lanying_redis.get_redis_connection()
    fields = site_statistics_fields()
    columns = []
    now_date = start_date
    cnt = 0
    while now_date <= end_date and cnt < 365 * 3:
        columns.append(now_date.strftime('%Y-%m-%d'))
        cnt += 1
        now_date = now_date + datetime_timedelta(1)
    values = {}
    for target in targets:
        target_values = []
        if target in fields:
            statistic_key = site_statistics_key(app_id, site_id, target)
            info = lanying_redis.redis_hgetall(redis, statistic_key)
        else:
            info = {}
        for column in columns:
            target_values.append(int(info.get(column, 0)))
        values[target] = target_values
    return {
        'result': 'ok',
        'data': {
            'columns': columns,
            'values': values
        }
    }

def update_site_acc_statistics(app_id, site_id):
    info = get_site_all_statistics(app_id, site_id)
    update_site_field(app_id, site_id, 'statistics_update_time', lanying_utils.get_time_str())
    if 'newUsers' in info:
        sum = 0
        for _,value in info['newUsers'].items():
            try:
                sum += int(value)
            except Exception as e:
                pass
        update_site_field(app_id, site_id, 'total_new_users', sum)
    if 'activeUsers' in info:
        sum = 0
        for _,value in info['activeUsers'].items():
            try:
                sum += int(value)
            except Exception as e:
                pass
        update_site_field(app_id, site_id, 'total_active_users', sum)
    if 'screenPageViews' in info:
        sum = 0
        for _,value in info['screenPageViews'].items():
            try:
                sum += int(value)
            except Exception as e:
                pass
        update_site_field(app_id, site_id, 'total_page_views', sum)

def update_all_site_statistics():
    info = get_all_site_list()
    for site_id, app_id in info.items():
        update_site_statistics(app_id, site_id)

def schedule_update_all_site_statistics():
    logging.info("schedule_update_all_site_statistics start")
    site_schedules = []
    for site_id, app_id in get_all_site_list().items():
        site = get_site(app_id, site_id)
        if site:
            if 'google_analytics_property_name' in site:
                site_schedules.append((site_id, app_id))
    logging.info(f"schedule_update_all_site_statistics site list size: {len(site_schedules)}")
    if len(site_schedules) > 0:
        max_delay = min(60, max(30,round(3600 * 1 / len(site_schedules))))
        min_delay = max(5, round(max_delay / 2))
        from lanying_tasks import site_statistics_task
        delay = random.randint(1, 20)
        logging.info(f"schedule_update_all_site_statistics schedule delay: {delay}")
        site_statistics_task.apply_async(args = [site_schedules, min_delay, max_delay, 1], countdown=delay)

def do_site_statistics_task(site_schedules, min_delay, max_delay, index):
    if len(site_schedules) > 0:
        site_id, app_id = site_schedules[0]
        logging.info(f"do_site_statistics_task start | app_id:{app_id}, site_id:{site_id}, min_delay:{min_delay}, max_delay:{max_delay}, index:{index}")
        try:
            update_site_statistics(app_id, site_id)
        except Exception as e:
            logging.exception(e)
        site_schedules = site_schedules[1:]
        if len(site_schedules) > 0:
            from lanying_tasks import site_statistics_task
            delay = random.randint(min_delay, max_delay)
            logging.info(f"do_site_statistics_task schedule delay: {delay}")
            site_statistics_task.apply_async(args = [site_schedules, min_delay, max_delay, index+1], countdown=delay)

def schedule_update_all_site_baidu_index():
    logging.info("schedule_update_all_site_baidu_index start")
    tasks = []
    for site_id, app_id in get_all_site_list().items():
        site = get_site(app_id, site_id)
        if site:
            task = {
                'site_id': site_id,
                'app_id': app_id,
                'times': 0
            }
            tasks.append(task)
    logging.info(f"schedule_update_all_site_baidu_index tasks list size: {len(tasks)}")
    if len(tasks) > 0:
        max_delay = min(180, max(30,round(3600 * 4 / len(tasks))))
        min_delay = max(5, round(max_delay / 2))
        from lanying_tasks import site_baidu_index_task
        delay = random.randint(1, 20)
        logging.info(f"schedule_update_all_site_baidu_index schedule delay: {delay}")
        site_baidu_index_task.apply_async(args = [tasks, min_delay, max_delay, 1], countdown=delay)

def do_site_baidu_index_task(site_schedules, min_delay, max_delay, index):
    if len(site_schedules) > 0:
        schedule = site_schedules[0]
        app_id = schedule['app_id']
        site_id = schedule['site_id']
        times = schedule['times']
        logging.info(f"do_site_baidu_index_task start | app_id:{app_id}, site_id:{site_id}, times:{times}, min_delay:{min_delay}, max_delay:{max_delay}, index:{index}")
        need_delay = False
        need_retry = False
        try:
            site = get_site(app_id, site_id)
            if site:
                canonical_domain = get_site_hostname(site.get('canonical_link',''))
                if canonical_domain != '' and 'site.chatai101.com' not in canonical_domain:
                    need_delay = True
                    logging.info(f"do_site_baidu_index_task baidu start | app_id:{app_id}, site_id:{site_id}, times:{times}, canonical_domain:{canonical_domain}")
                    result = lanying_baidu.check_baidu_index(canonical_domain)
                    logging.info(f"do_site_baidu_index_task baidu finish | app_id:{app_id}, site_id:{site_id}, times:{times}, canonical_domain:{canonical_domain}, result:{result}")
                    if result['result'] == 'ok':
                        update_site_field(app_id, site_id, 'baidu_index_pages', result['count'])
                        update_site_field(app_id, site_id, 'baidu_index_update_time', lanying_utils.get_time_str())
                        update_site_field(app_id, site_id, 'baidu_index_domain', canonical_domain)
                    else:
                        need_retry = True
        except Exception as e:
            logging.exception(e)
        site_schedules = site_schedules[1:]
        if need_retry:
            times += 1
            if times <= 5:
                schedule['times'] = times
                site_schedules.append(schedule)
                logging.info(f"do_site_baidu_index_task schedule retry | app_id:{app_id}, site_id:{site_id}, times:{times}")
            else:
                logging.info(f"do_site_baidu_index_task stop retry | app_id:{app_id}, site_id:{site_id}, times:{times}")
        if len(site_schedules) > 0:
            from lanying_tasks import site_baidu_index_task
            if need_delay:
                delay = random.randint(min_delay, max_delay)
            else:
                delay = 1
            logging.info(f"do_site_baidu_index_task schedule delay: {delay}")
            site_baidu_index_task.apply_async(args = [site_schedules, min_delay, max_delay, index+1], countdown=delay)

def schedule_update_all_site_google_index():
    logging.info("schedule_update_all_site_google_index start")
    tasks = []
    for site_id, app_id in get_all_site_list().items():
        if is_deduct_failed(app_id):
            logging.info(f"schedule_update_all_site_google_index skip deduct failed app_id:{app_id}")
            continue
        site = get_site(app_id, site_id)
        if site:
            task = {
                'site_id': site_id,
                'app_id': app_id,
                'times': 0
            }
            tasks.append(task)
    logging.info(f"schedule_update_all_site_google_index tasks list size: {len(tasks)}")
    if len(tasks) > 0:
        max_delay = min(180, max(30,round(3600 * 4 / len(tasks))))
        min_delay = max(5, round(max_delay / 2))
        from lanying_tasks import site_google_index_task
        delay = random.randint(1, 20)
        logging.info(f"schedule_update_all_site_google_index schedule delay: {delay}")
        site_google_index_task.apply_async(args = [tasks, min_delay, max_delay, 1], countdown=delay)

def do_site_google_index_task(site_schedules, min_delay, max_delay, index):
    if len(site_schedules) > 0:
        schedule = site_schedules[0]
        app_id = schedule['app_id']
        site_id = schedule['site_id']
        times = schedule['times']
        logging.info(f"do_site_google_index_task start | app_id:{app_id}, site_id:{site_id}, times:{times}, min_delay:{min_delay}, max_delay:{max_delay}, index:{index}")
        need_delay = False
        need_retry = False
        try:
            site = get_site(app_id, site_id)
            if site:
                canonical_domain = get_site_hostname(site.get('canonical_link',''))
                if canonical_domain != '' and 'site.chatai101.com' not in canonical_domain:
                    need_delay = True
                    logging.info(f"do_site_google_index_task google start | app_id:{app_id}, site_id:{site_id}, times:{times}, canonical_domain:{canonical_domain}")
                    result = lanying_google.check_index(canonical_domain)
                    logging.info(f"do_site_google_index_task google finish | app_id:{app_id}, site_id:{site_id}, times:{times}, canonical_domain:{canonical_domain}, result:{result}")
                    if result['result'] == 'ok':
                        update_site_field(app_id, site_id, 'google_index_pages', result['count'])
                        update_site_field(app_id, site_id, 'google_index_update_time', lanying_utils.get_time_str())
                        update_site_field(app_id, site_id, 'google_index_domain', canonical_domain)
                    else:
                        need_retry = True
        except Exception as e:
            logging.exception(e)
        site_schedules = site_schedules[1:]
        if need_retry:
            times += 1
            if times <= 5:
                schedule['times'] = times
                site_schedules.append(schedule)
                logging.info(f"do_site_google_index_task schedule retry | app_id:{app_id}, site_id:{site_id}, times:{times}")
            else:
                logging.info(f"do_site_google_index_task stop retry | app_id:{app_id}, site_id:{site_id}, times:{times}")
        if len(site_schedules) > 0:
            from lanying_tasks import site_google_index_task
            if need_delay:
                delay = random.randint(min_delay, max_delay)
            else:
                delay = 1
            logging.info(f"do_site_google_index_task schedule delay: {delay}")
            site_google_index_task.apply_async(args = [site_schedules, min_delay, max_delay, index+1], countdown=delay)

def format_statistics_date(date_str):
    date_obj = datetime.strptime(date_str, '%Y%m%d')
    return date_obj.strftime('%Y-%m-%d')

def site_statistics_key(app_id, site_id, category):
    return f'lanying_connector:grow_ai:site_statistics:{app_id}:{site_id}:{category}'

def check_site_setting(site_setting: SiteSetting):
    if site_setting.type not in ["gitbook"]:
        return {'result': 'error', 'message': 'invalid site type'}
    if site_setting.github_hosting == 'on':
        return {'result': 'ok'}
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

def maybe_init_github_site_repo(app_id, old_site_info, site_info):
    if site_info['github_hosting'] == 'on':
        site_id = site_info['site_id']
        if 'github_hosting_url' in site_info and site_info['github_hosting_url'] != '':
            if old_site_info['github_hosting'] == 'off':
                update_site_field(app_id, site_id, "github_url", site_info['github_hosting_url'])
                update_site_field(app_id, site_id, "github_token", '')
                update_site_field(app_id, site_id, "github_base_branch", 'master')
                site_info = get_site(app_id, site_id)
            return site_info
        repo_org = get_github_org()
        repo_name = f'growai-gitbook-{site_id}-{int(time.time())}'
        repo_full_name = f'{repo_org}/{repo_name}'
        try:
            logging.info(f"create github repo start | repo_name:{repo_name}")
            github = get_github()
            my_org = github.get_organization(get_github_org())
            template = github.get_repo("maxim-top/growai-gitbook")
            result = my_org.create_repo_from_template(
                repo=template,
                name=repo_name,
                description="Create from maxim-top/growai-gitbook",
                private=False,
                include_all_branches=False
            )
            logging.info(f"create github repo finish | app_id:{app_id}, repo_full_name:{repo_full_name}, result:{result}")
            ## enable_workflow_action(f'{repo_org}/{repo_name}')
            github_url = f'https://github.com/{repo_full_name}'
            update_site_field(app_id, site_id, "github_hosting_url", github_url)
            update_site_field(app_id, site_id, "github_hosting_org", repo_org)
            update_site_field(app_id, site_id, "github_url", github_url)
            update_site_field(app_id, site_id, "github_token", '')
            update_site_field(app_id, site_id, "github_base_branch", 'master')
            update_site_field(app_id, site_id, "github_base_dir", '/')
            site_info = get_site(app_id, site_id)
            lanying_slack.async_send_grafana_message(f'create github repo success: app_id:{app_id}, repo_full_name: {repo_full_name}')
        except Exception as e:
            logging.error(f'create github repo failed: {repo_name}')
            logging.exception(e)
            lanying_slack.async_send_grafana_message(f'create github repo failed: app_id:{app_id}, repo_full_name: {repo_full_name}')
    return site_info

# def enable_workflow_action(repo_full_name):
#     try:
#         token = get_github_token()
#         HEADERS = {
#             "Authorization": f"token {token}",
#             "Accept": "application/vnd.github+json"
#         }
#         url = f"https://api.github.com/repos/{repo_full_name}/actions/permissions"
#         resp = requests.put(
#             url, headers=HEADERS,
#             json={
#             "enabled": True,
#             "allowed_actions": "all"
#             }
#         )
#         if resp.status_code == 204:
#             return {'result': 'ok'}
#         else:
#             logging.info(f"enable_workflow_action failed | resp:{resp.content}")
#             return {'result': 'error', 'message': 'bad_status_code'}
#     except Exception as e:
#         return {'result': 'error', 'message': 'exception'}

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

def check_domain_owner(app_id, site_id, domain_name):
    site = get_site(app_id, site_id)
    if site is None:
        return {
            'result': 'error',
            'message': 'site_not_found'
        }
    if not is_valid_domain(domain_name):
        return {
            'result': 'error',
            'message': 'domain_name_invalid'
        }
    if is_domain_name_reserved(domain_name):
        return {
            'result': 'error',
            'message': 'domain_name_is_reserved'
        }
    result = lanying_cdn.verify_domain_owner(domain_name)
    if result == True:
        return {
            'result': 'ok',
            'data': {
                'success': True
            }
        }
    result = lanying_cdn.describe_domain_verify_data(domain_name)
    return result

def is_valid_domain(domain):
    # 正则表达式用于匹配合法的域名
    pattern = r'^(?:[A-Za-z0-9](?:[A-Za-z0-9-]{0,61}[A-Za-z0-9])?\.)+[A-Za-z0-9-]{2,}$'
    return bool(re.match(pattern, domain))

def is_domain_name_reserved(domain_name):
    # if 'lanyingim.com' in domain_name:
    #     return True
    # if 'maximtop.com' in domain_name:
    #     return True
    # if 'chatai101.com' in domain_name:
    #     return True
    # if 'maxim.top' in domain_name:
    #     return True
    # if 'maximtop.cn' in domain_name:
    #     return True
    # if 'maximtop.com.cn' in domain_name:
    #     return True
    return False

def get_cdn_source_domain():
    return os.getenv('LANYING_CONNECTOR_GROW_AI_CDN_SOURCE_DOMAIN')

def get_custom_domain_info_key(app_id, site_id, domain_id):
    return f'lanying_connector:custom_domain_info:{app_id}:{site_id}:{domain_id}'
def get_custom_domain_name_key(domain_name):
    return f'lanying_connector:custom_domain_name:{domain_name}'

def generate_custom_domain_id():
    redis = lanying_redis.get_redis_connection()
    return redis.incrby("lanying_connector:grow_ai:custom_domain_id_generator", 1)

def create_custom_domain_info(domain_name, app_id, site_id, site_name, scope):
    now = int(time.time())
    domain_id = generate_custom_domain_id()
    redis = lanying_redis.get_redis_connection()
    redis.hmset(get_custom_domain_info_key(app_id, site_id, domain_id), {
        'domain_id': domain_id,
        'domain_name': domain_name,
        'app_id': app_id,
        'site_id': site_id,
        'create_time': now,
        'state': 'wait_cname',
        'site_name': site_name,
        'scope': scope
    })
    redis.hmset(get_custom_domain_name_key(domain_name), {
        'domain_id': domain_id,
        'domain_name': domain_name,
        'app_id': app_id,
        'site_id': site_id,
    })
    return domain_id

def update_custom_domain_info(app_id, site_id, domain_id, field, value):
    redis = lanying_redis.get_redis_connection()
    redis.hset(get_custom_domain_info_key(app_id, site_id, domain_id), field, value)

def get_custom_domain_name(domain_name):
    redis = lanying_redis.get_redis_connection()
    return lanying_redis.redis_hgetall(redis, get_custom_domain_name_key(domain_name))

def get_custom_domain_info(app_id, site_id, domain_id):
    redis = lanying_redis.get_redis_connection()
    return lanying_redis.redis_hgetall(redis, get_custom_domain_info_key(app_id, site_id, domain_id))

def check_domain_num(app_id, site_id, max_domain_num):
    site_list = get_site_list(app_id)['data']['list']
    domain_num = 1
    for site in site_list:
        if site['site_id'] != site_id:
            if site['domain_id'] != '':
                domain_num += 1
    if domain_num > max_domain_num:
        logging.info(f"check_domain_num error | app_id:{app_id}, site_id:{site_id}, domain_num:{domain_num}, max_domain_num:{max_domain_num}")
        return {
            'result': 'error',
            'message': f'domain_num_exceed({domain_num}/{max_domain_num})'
        }
    else:
        logging.info(f"check_domain_num ok | app_id:{app_id}, site_id:{site_id}, domain_num:{domain_num}, max_domain_num:{max_domain_num}")
        return {
            'result': 'ok'
        }

def domain_num_limit_changed(app_id, max_domain_num, tenement_id):
    site_list = get_site_list(app_id)['data']['list']
    domain_count = 0
    for site in site_list:
        site_id = site['site_id']
        domain_id = site.get('domain_id', '')
        if domain_id != '':
            domain_info = get_custom_domain_info(app_id, site_id, domain_id)
            if domain_info:
                cdn_status = domain_info.get('cdn_status', 'online')
                if cdn_status == 'online':
                    domain_count += 1
                    if domain_count > max_domain_num:
                        try:
                            domain_name = domain_info['domain_name']
                            lanying_cdn.stop_cdn(domain_name)
                            update_custom_domain_info(app_id, site_id, domain_id, 'cdn_status', 'offline')
                            logging.info(f"domain_num_limit_changed stop cdn success| app_id:{app_id}, max_domain_num:{max_domain_num}, site_id:{site_id}, domain_id:{domain_id}, domain_name:{domain_name}")
                            lanying_slack.async_send_message(f'GrowAI 停止CDN成功。租户ID: {tenement_id}, app_id:{app_id}, max_domain_num:{max_domain_num}, site_id:{site_id}, domain_id:{domain_id}, domain_name:{domain_name}')
                        except Exception as e:
                            logging.exception(e)
                            logging.info(f"domain_num_limit_changed stop cdn failed| app_id:{app_id}, max_domain_num:{max_domain_num}, site_id:{site_id}, domain_id:{domain_id}, domain_name:{domain_name}")
                            lanying_slack.async_send_message(f'GrowAI 停止CDN失败。租户ID: {tenement_id}, app_id:{app_id}, max_domain_num:{max_domain_num}, site_id:{site_id}, domain_id:{domain_id}, domain_name:{domain_name}')
                elif cdn_status == 'offline':
                    if domain_count + 1 <= max_domain_num:
                        domain_count += 1
                        try:
                            domain_name = domain_info['domain_name']
                            lanying_cdn.start_cdn(domain_name)
                            update_custom_domain_info(app_id, site_id, domain_id, 'cdn_status', 'online')
                            logging.info(f"domain_num_limit_changed start cdn success| app_id:{app_id}, max_domain_num:{max_domain_num}, site_id:{site_id}, domain_id:{domain_id}, domain_name:{domain_name}")
                            lanying_slack.async_send_message(f'GrowAI 启动CDN成功。租户ID: {tenement_id}, app_id:{app_id}, max_domain_num:{max_domain_num}, site_id:{site_id}, domain_id:{domain_id}, domain_name:{domain_name}')
                        except Exception as e:
                            logging.exception(e)
                            logging.info(f"domain_num_limit_changed start cdn failed| app_id:{app_id}, max_domain_num:{max_domain_num}, site_id:{site_id}, domain_id:{domain_id}, domain_name:{domain_name}")
                            lanying_slack.async_send_message(f'GrowAI 启动CDN失败。租户ID: {tenement_id}, app_id:{app_id}, max_domain_num:{max_domain_num}, site_id:{site_id}, domain_id:{domain_id}, domain_name:{domain_name}')
    return {
        'result': 'ok',
        'data': {
            'success': True,
            'domain_count': domain_count
        }
    }

def create_custum_domain(app_id, site_id, domain_name, scope, tenement_id, check_verify_owner, max_domain_num):
    site = get_site(app_id, site_id)
    if site is None:
        return {
            'result': 'error',
            'message': 'site_not_exist'
        }
    if 'site_name' not in site:
        return {
            'result': 'error',
            'message': 'site_name_not_exist'
        }
    site_name = site['site_name']
    result = check_domain_num(app_id, site_id, max_domain_num)
    if result['result'] == 'error':
        return result
    result = check_domain_owner(app_id, site_id, domain_name)
    if result['result'] == 'error':
        return result
    if 'success' not in result['data']:
        if check_verify_owner == 'on':
            return {
                'result': 'error',
                'message': 'site_owner_verify_failed'
            }
        else:
            return result
    result = lanying_cdn.add_cdn(domain_name, get_cdn_source_domain(), scope)
    if result['result'] == 'error':
        return result
    domain_id = create_custom_domain_info(domain_name, app_id, site_id, site_name, scope)
    update_site_field(app_id, site_id, 'domain_id', domain_id)
    clean_old_domain(app_id, site_id, site, tenement_id)
    domain_info = get_custom_domain_info(app_id, site_id, domain_id)
    lanying_slack.async_send_message(f'GrowAI 开始创建CDN, 租户ID: {tenement_id}, app_id:{app_id}, site_id:{site_id}, domain_id:{domain_id}, domain_name:{domain_name}, scope:{scope}')
    return {
        'result': 'ok',
        'data': domain_info
    }

def clean_old_domain(app_id, site_id, old_site, tenement_id):
    try:
        if old_site['domain_id'] != '':
            old_domain_id = old_site['domain_id']
            old_domain_info = get_custom_domain_info(app_id, site_id, old_domain_id)
            if old_domain_info:
                domain_name = old_domain_info['domain_name']
                try:
                    lanying_cdn.delete_cdn(domain_name)
                    lanying_slack.async_send_message(f'GrowAI 删除CDN成功, 租户ID: {tenement_id}, app_id:{app_id}, site_id:{site_id}, domain_id:{old_domain_id}, domain_name:{domain_name}')
                except Exception as e:
                    logging.exception(e)
                    lanying_slack.async_send_message(f'GrowAI 删除CDN失败, 租户ID: {tenement_id}, app_id:{app_id}, site_id:{site_id}, domain_id:{old_domain_id}, domain_name:{domain_name}')
    except Exception as e:
        logging.exception(e)

def manual_clean_site_domain(app_id, site_id):
    site = get_site(app_id, site_id)
    result = {}
    if site['domain_id'] != '':
        domain_id = site['domain_id']
        result['domain_id'] = domain_id
        domain_info = get_custom_domain_info(app_id, site_id, domain_id)
        if domain_info:
            domain_name = domain_info['domain_name']
            result['domain_name'] = domain_name
            try:
                lanying_cdn.delete_cdn(domain_name)
                result['delete_cdn_result'] = 'ok'
            except Exception as e:
                result['delete_cdn_result'] = 'error'
        update_site_field(app_id, site_id, 'domain_id', '')
    return result

def get_site_custom_domain_info_list(app_id):
    site_list = get_site_list(app_id)['data']['list']
    domain_info_list = []
    for site in site_list:
        site_id = site['site_id']
        domain_id = site.get('domain_id', '')
        if domain_id != '':
            domain_info = get_custom_domain_info(app_id, site_id, domain_id)
            if domain_info:
                domain_info_list.append(domain_info)
    return {
        'result': 'ok',
        'data': {
            'list': domain_info_list
        }
    }

def check_domain_cname(app_id, site_id, tenement_id):
    site = get_site(app_id, site_id)
    if site is None:
        return {
            'result': 'error',
            'message': 'site_not_exist'
        }
    domain_id = site.get('domain_id', '')
    if domain_id == '':
        return {
            'result': 'error',
            'message': 'domain_not_exist'
        }
    domain_info = get_custom_domain_info(app_id, site_id, domain_id)
    if domain_info is None:
        return {
            'result': 'error',
            'message': 'domain_not_exist'
        }
    if 'cname' not in domain_info:
        return {
            'result': 'error',
            'message': 'cname_value_not_exist'
        }
    domain_name = domain_info['domain_name']
    if 'cname_ready' not in domain_info:
        try:
            res = lanying_cdn.desc_cdn_cname(domain_name)
            ready = False
            for i in res.body.cname_datas.data:
                if i.status == 0:
                    ready = True
            if ready:
                update_custom_domain_info(app_id, site_id, domain_id, "cname_ready", 'ready')
                update_custom_domain_info(app_id, site_id, domain_id, "state", 'wait_cdn_config')
                update_custom_domain_info(app_id, site_id, domain_id, "task_status", "wait")
                custom_site_url = f'https://{domain_name}/'
                update_site_field(app_id, site_id, "custom_site_name", domain_name)
                update_site_field(app_id, site_id, "custom_site_url", custom_site_url)
                old_canonical_link =  site.get('old_canonical_link', '')
                old_custom_site_url= site.get('custom_site_url', '')
                if old_canonical_link == '' or 'docs.lanyingim.com' in old_canonical_link or '.site.chatai101.com' in old_canonical_link or old_custom_site_url == old_canonical_link:
                    update_site_field(app_id, site_id, "canonical_link", custom_site_url)
                lanying_slack.async_send_message(f'GrowAI 开始配置CDN, 租户ID: {tenement_id}, app_id:{app_id}, site_id:{site_id}, domain_id:{domain_id}, domain_name:{domain_name}')
                from lanying_tasks import grow_ai_cdn_config_task_run
                grow_ai_cdn_config_task_run.apply_async(args = [app_id, site_id, domain_id, tenement_id], countdown=10)
                domain_info = get_custom_domain_info(app_id, site_id, domain_id)
                return {
                    'result': 'ok',
                    'data': domain_info
                }
        except Exception as e:
            pass
        return {
            'result': 'error',
            'message': 'cname_not_ready'
        }
    else:
        return {
            'result': 'ok',
            'data': domain_info
        }

def get_site_custom_domain_info(app_id, site_id):
    site = get_site(app_id, site_id)
    if site is None:
        return {
            'result': 'error',
            'message': 'site_not_exist'
        }
    domain_id = site.get('domain_id', '')
    if domain_id == '':
        return {
            'result': 'error',
            'message': 'domain_not_exist'
        }
    domain_info = get_custom_domain_info(app_id, site_id, domain_id)
    if domain_info is None:
        return {
            'result': 'error',
            'message': 'domain_not_exist'
        }
    if 'cname' in domain_info:
        return {
            'result': 'ok',
            'data': domain_info
        }
    domain_name = domain_info['domain_name']
    try:
        res = lanying_cdn.desc_cdn(domain_name)
        cname = res.body.get_domain_detail_model.cname
        if len(cname) > 0:
            update_custom_domain_info(app_id, site_id, domain_id, "cname", cname)
            domain_info = get_custom_domain_info(app_id, site_id, domain_id)
            return {
                'result': 'ok',
                'data': domain_info
            }
    except Exception as e:
        pass
    return {
        'result': 'ok',
        'data': domain_info
    }

def do_cdn_config_task_run(app_id, site_id, domain_id, tenement_id, has_retry_times, now_times, max_times):
    try:
        site = get_site(app_id, site_id)
        if site is None:
            return {
                'result': 'error',
                'message': 'site_not_exist'
            }
        if domain_id != site.get('domain_id', ''):
            return {
                'result': 'error',
                'message': 'domain_id_changed'
            }
        domain_info = get_custom_domain_info(app_id, site_id, domain_id)
        if domain_info is None:
            return {
                'result': 'error',
                'message': 'domain_not_exist'
            }
        if domain_info['state'] != 'wait_cdn_config':
            return {
                'result': 'error',
                'message': 'bad_domain_state'
            }
        domain_name = domain_info['domain_name']
        cert_failed_times = domain_info.get('cert_failed_times', '0')
        logging.info(f"cdn_config_task_run_internal start | app_id:{app_id}, site_id:{site_id}, domain_id:{domain_id}, domain_name:{domain_name}, progress:{now_times}/{max_times}")
        result = do_cdn_config_task_run_internal(app_id, site_id, domain_id, domain_info, tenement_id)
        if result['result'] == 'ok':
            logging.info(f"cdn_config_task_run_internal success | app_id:{app_id}, site_id:{site_id}, domain_id:{domain_id}, domain_name:{domain_name}, progress:{now_times}/{max_times}, result:{result}, cert_failed_times:{cert_failed_times}")
            lanying_slack.async_send_message(f'GrowAI 配置CDN完成, 租户ID: {tenement_id}, app_id:{app_id}, site_id:{site_id}, domain_id:{domain_id}, domain_name:{domain_name}, domain_id:{domain_id}, try_times:{now_times}/{max_times}, cert_failed_times:{cert_failed_times}')
            update_custom_domain_info(app_id, site_id, domain_id, "task_status", "success")
            return result
        else:
            retry = result.get('retry', True)
            if retry:
                if has_retry_times:
                    update_custom_domain_info(app_id, site_id, domain_id, "task_status", "retry")
                else:
                    logging.info(f"cdn_config_task_run_internal failed with no retry times | app_id:{app_id}, site_id:{site_id}, domain_id:{domain_id}, domain_name:{domain_name}, progress:{now_times}/{max_times}, result:{result}, cert_failed_times:{cert_failed_times}")
                    lanying_slack.async_send_message(f'GrowAI 配置CDN失败, 租户ID: {tenement_id}, app_id:{app_id}, site_id:{site_id}, domain_id:{domain_id}, domain_name:{domain_name}, try_times:{now_times}/{max_times}, result:{result}, cert_failed_times:{cert_failed_times}')
                    update_custom_domain_info(app_id, site_id, domain_id, "task_status", "error")
                raise Exception(result)
            else:
                logging.info(f"cdn_config_task_run_internal failed with not retry | app_id:{app_id}, site_id:{site_id}, domain_id:{domain_id}, domain_name:{domain_name}, progress:{now_times}/{max_times}, result:{result}, cert_failed_times:{cert_failed_times}")
                lanying_slack.async_send_message(f'GrowAI 配置CDN失败, 租户ID: {tenement_id}, app_id:{app_id}, site_id:{site_id}, domain_id:{domain_id}, domain_name:{domain_name}, try_times:{now_times}/{max_times}, result:{result}, cert_failed_times:{cert_failed_times}')
                return result
    except Exception as e:
        logging.exception(e)
        raise e

def do_cdn_config_task_run_internal(app_id, site_id, domain_id, domain_info, tenement_id):
    domain_name = domain_info['domain_name']
    logging.info(f"do_cdn_config_task_run_internal start | app_id:{app_id}, site_id:{site_id}, domain_id:{domain_id}, domain_name:{domain_name}")
    if 'cdn_ready' not in domain_info:
        try:
            res = lanying_cdn.desc_cdn(domain_name)
            if res.body.get_domain_detail_model.domain_status == 'online':
                logging.info(f"do_cdn_config_task_run_internal cdn is ready | app_id:{app_id}, site_id:{site_id}, domain_id:{domain_id}, domain_name:{domain_name}")
                update_custom_domain_info(app_id, site_id, domain_id, "cdn_ready", "ready")
            else:
                logging.info(f"do_cdn_config_task_run_internal cdn is not ready | app_id:{app_id}, site_id:{site_id}, domain_id:{domain_id}, domain_name:{domain_name}")
                return {
                    'result': 'error',
                    'message': 'cdn_not_ready',
                    'retry': True
                }
        except Exception as e:
            logging.exception(e)
            logging.info(f"do_cdn_config_task_run_internal cdn is not ready, got exception | app_id:{app_id}, site_id:{site_id}, domain_id:{domain_id}, domain_name:{domain_name}")
            return {
                'result': 'error',
                'message': 'cdn_not_ready',
                'retry': True
            }
    if 'cdn_config_ready' not in domain_info:
        try:
            res = lanying_cdn.set_cdn_domain_config(domain_name,get_cdn_source_domain(), domain_info['site_name'])
            update_custom_domain_info(app_id, site_id, domain_id, "cdn_config_ready", "ready")
            logging.info(f"do_cdn_config_task_run_internal cdn config finish | app_id:{app_id}, site_id:{site_id}, domain_id:{domain_id}, domain_name:{domain_name}")
        except Exception as e:
            logging.exception(e)
            logging.info(f"do_cdn_config_task_run_internal cdn config exception | app_id:{app_id}, site_id:{site_id}, domain_id:{domain_id}, domain_name:{domain_name}")
            return {
                'result': 'error',
                'message': 'cdn_config_got_exception',
                'retry': False
            }
    if 'http_ready' not in domain_info:
        test_key = f'{app_id}_{site_id}_{domain_id}'
        test_value = f'{site_id}_{domain_id}'
        lanying_cert.set_acme_challenge_value(test_key,test_value)
        try:
            url = f'http://{domain_name}/.well-known/acme-challenge/{test_key}'
            logging.info(f"do_cdn_config_task_run_internal http check start | app_id:{app_id}, site_id:{site_id}, domain_id:{domain_id}, domain_name:{domain_name}, url: {url}")
            response = requests.get(url)
            logging.info(f"do_cdn_config_task_run_internal http check response | app_id:{app_id}, site_id:{site_id}, domain_id:{domain_id}, domain_name:{domain_name}, response: {response.text}")
            if response.text == test_value:
                update_custom_domain_info(app_id, site_id, domain_id, "http_ready", "ready")
                logging.info(f"do_cdn_config_task_run_internal http check success | app_id:{app_id}, site_id:{site_id}, domain_id:{domain_id}, domain_name:{domain_name}")
            else:
                logging.info(f"do_cdn_config_task_run_internal http check failed | app_id:{app_id}, site_id:{site_id}, domain_id:{domain_id}, domain_name:{domain_name}")
                return {
                    'result': 'error',
                    'message': 'http_not_ready',
                    'retry': True
                }
        except Exception as e:
            logging.exception(e)
            logging.info(f"do_cdn_config_task_run_internal http check exception | app_id:{app_id}, site_id:{site_id}, domain_id:{domain_id}, domain_name:{domain_name}")
            return {
                'result': 'error',
                'message': 'http_not_ready',
                'retry': True
            }
    if 'cert_ready' not in domain_info:
        try:
            logging.info(f"do_cdn_config_task_run_internal start cert request | app_id:{app_id}, site_id:{site_id}, domain_id:{domain_id}, domain_name:{domain_name}")
            client_acme = lanying_cert.get_acme_client()
            pkey_pem, csr_pem = lanying_cert.new_csr_comp(domain_name)
            orderr = client_acme.new_order(csr_pem)
            try:
                logging.info(f"do_cdn_config_task_run_internal cert start challenge cert order | app_id:{app_id}, site_id:{site_id}, domain_id:{domain_id}, domain_name:{domain_name}")
                challb = lanying_cert.select_http01_chall(orderr)
                finalized_orderr = lanying_cert.perform_http01(client_acme, challb, orderr)
                logging.info(f"do_cdn_config_task_run_internal cert finish | app_id:{app_id}, site_id:{site_id}, domain_id:{domain_id}, domain_name:{domain_name}")
                update_custom_domain_info(app_id, site_id, domain_id, "cert_pem", finalized_orderr.fullchain_pem)
                update_custom_domain_info(app_id, site_id, domain_id, "cert_key", pkey_pem)
                cert_create_time = int(time.time())
                update_custom_domain_info(app_id, site_id, domain_id, "cert_create_time", cert_create_time)
                update_custom_domain_info(app_id, site_id, domain_id, "cert_ready", "ready")
                add_custom_domain_renew_schedule(app_id, site_id, domain_id, domain_name, cert_create_time, tenement_id)
                domain_info = get_custom_domain_info(app_id, site_id, domain_id)
            except Exception as e:
                logging.exception(e)
                logging.info(f"do_cdn_config_task_run_internal cert exception | app_id:{app_id}, site_id:{site_id}, domain_id:{domain_id}, domain_name:{domain_name}")
                cert_failed_times = int(domain_info.get('cert_failed_times', '0')) + 1
                update_custom_domain_info(app_id, site_id, domain_id, "cert_failed_times", cert_failed_times)
                if cert_failed_times < 6:
                    return {
                        'result': 'error',
                        'message': 'cert_not_ready',
                        'retry': True,
                        'retry_delay_time': 60 * cert_failed_times
                    }
                else:
                    return {
                        'result': 'error',
                        'message': 'cert_not_ready',
                        'retry': False
                    }
        except Exception as e:
            logging.exception(e)
            logging.info(f"do_cdn_config_task_run_internal cert order exception | app_id:{app_id}, site_id:{site_id}, domain_id:{domain_id}, domain_name:{domain_name}")
            return {
                'result': 'error',
                'message': 'cert_not_ready',
                'retry': True,
                'retry_delay_time': 60
            }
    try:
        lanying_cdn.set_cdn_domain_cert(domain_name, domain_info['cert_pem'], domain_info['cert_key'])
        update_custom_domain_info(app_id, site_id, domain_id, "state", 'ready')
        update_custom_domain_info(app_id, site_id, domain_id, "cert_ready", "ready")
        maybe_init_analytics(app_id, site_id)
        site = get_site(app_id, site_id)
        if site:
            maybe_sync_to_github(site,site)
        return {
            'result': 'ok'
        }
    except Exception as e:
        logging.exception(e)
        return {
            'result': 'error',
            'message': 'cert_config_not_ready',
            'retry': True
        }

def add_custom_domain_renew_schedule(app_id, site_id, domain_id, domain_name, cert_create_time, tenement_id):
    redis = lanying_redis.get_redis_connection()
    value = {
        'app_id': app_id,
        'site_id': site_id,
        'domain_id': domain_id,
        'domain_name': domain_name,
        'cert_create_time': cert_create_time,
        'tenement_id': tenement_id
    }
    redis.hset(custom_domain_renew_schedule_key(), f'{app_id}_{site_id}', json.dumps(value))

def custom_domain_renew_schedule_key():
    return 'lanying-connector:custom_domain_renew_schedule'

def custom_domain_run_renew_schedules():
    logging.info("custom_domain_run_renew_schedules start")
    redis = lanying_redis.get_redis_connection()
    schedules = lanying_redis.redis_hgetall(redis, custom_domain_renew_schedule_key())
    check_time = int(time.time()) - 60 * 86400
    renew_schedules = []
    for _, schedule_str in schedules.items():
        try:
            schedule = json.loads(schedule_str)
            app_id = schedule['app_id']
            site_id = schedule['site_id']
            domain_id = schedule['domain_id']
            if is_deduct_failed(app_id):
                logging.info(f"custom_domain_run_renew_schedules skip deduct failed app_id:{app_id}，site_id:{site_id}")
                continue
            site = get_site(app_id, site_id)
            if site and domain_id == site.get('domain_id', ''):
                domain_info = get_custom_domain_info(app_id, site_id, domain_id)
                if domain_info:
                    cdn_status = domain_info.get('cdn_status', 'online')
                    if cdn_status == 'online':
                        cert_create_time = int(domain_info['cert_create_time'])
                        if cert_create_time < check_time:
                            renew_schedules.append(schedule)
        except Exception as e:
            logging.exception(e)
    logging.info(f"custom_domain_run_renew_schedules renew list size: {len(renew_schedules)}")
    if len(renew_schedules) > 0:
        max_delay = min(600, max(60,round(3600 * 8 / len(renew_schedules))))
        min_delay = max(60, round(max_delay / 2))
        from lanying_tasks import custom_domain_renew_task
        delay = random.randint(1, 20)
        logging.info(f"custom_domain_run_renew_schedules renew delay: {delay}")
        custom_domain_renew_task.apply_async(args = [renew_schedules, min_delay, max_delay, 1], countdown=delay)

def do_custom_domain_renew_task(renew_schedules, min_delay, max_delay, index):
    if len(renew_schedules) > 0:
        renew_schedule = renew_schedules[0]
        logging.info(f"do_custom_domain_renew_task start | renew_schedule:{renew_schedule}, min_delay:{min_delay}, max_delay:{max_delay}, index:{index}")
        try:
            do_custom_domain_renew(renew_schedule, index)
        except Exception as e:
            logging.exception(e)
        renew_schedules = renew_schedules[1:]
        if len(renew_schedules) > 0:
            from lanying_tasks import custom_domain_renew_task
            delay = random.randint(min_delay, max_delay)
            logging.info(f"do_custom_domain_renew_task renew delay: {delay}")
            custom_domain_renew_task.apply_async(args = [renew_schedules, min_delay, max_delay, index+1], countdown=delay)

def do_custom_domain_renew(schedule, index):
    check_time = int(time.time()) - 60 * 86400
    app_id = schedule['app_id']
    site_id = schedule['site_id']
    domain_id = schedule['domain_id']
    tenement_id = schedule['tenement_id']
    domain_name = schedule['domain_name']
    site = get_site(app_id, site_id)
    if site and domain_id == site.get('domain_id', ''):
        domain_info = get_custom_domain_info(app_id, site_id, domain_id)
        if domain_info:
            cdn_status = domain_info.get('cdn_status', 'online')
            if cdn_status == 'online':
                cert_create_time = int(domain_info['cert_create_time'])
                cert_renew_failed_times = domain_info.get('cert_renew_failed_times', '0')
                cert_days = round((int(time.time()) - cert_create_time) / 86400)
                if cert_create_time < check_time:
                    lanying_slack.async_send_message(f'GrowAI 开始续签证书, 租户ID: {tenement_id}, app_id:{app_id}, site_id:{site_id}, domain_id:{domain_id}, domain_name:{domain_name}, domain_id:{domain_id}, cert_failed_times:{cert_renew_failed_times}, cert_days:{cert_days}')
                    result = do_custom_domain_renew_internal(app_id, site_id, domain_id, domain_info, index, tenement_id)
                    if result['result'] == 'ok':
                        lanying_slack.async_send_message(f'GrowAI 续签证书成功, 租户ID: {tenement_id}, app_id:{app_id}, site_id:{site_id}, domain_id:{domain_id}, domain_name:{domain_name}, domain_id:{domain_id}, cert_failed_times:{cert_renew_failed_times}, cert_days:{cert_days}')
                    else:
                        lanying_slack.async_send_message(f'GrowAI 续签证书失败, 租户ID: {tenement_id}, app_id:{app_id}, site_id:{site_id}, domain_id:{domain_id}, domain_name:{domain_name}, domain_id:{domain_id}, cert_failed_times:{cert_renew_failed_times}, cert_days:{cert_days}, result:{result}')
                    return result
    return {
        'result': 'error',
        'message': 'no_need_renew'
    }

def do_custom_domain_renew_internal(app_id, site_id, domain_id, domain_info, index, tenement_id):
    domain_name = domain_info['domain_name']
    logging.info(f"do_custom_domain_renew_internal start | app_id:{app_id}, site_id:{site_id}, domain_id:{domain_id}, domain_name:{domain_name}, index:{index}")
    test_key = f'{app_id}_{site_id}_{domain_id}'
    test_value = f'{site_id}_{domain_id}'
    lanying_cert.set_acme_challenge_value(test_key,test_value)
    try:
        url = f'http://{domain_name}/.well-known/acme-challenge/{test_key}'
        logging.info(f"do_custom_domain_renew_internal http check start | app_id:{app_id}, site_id:{site_id}, domain_id:{domain_id}, domain_name:{domain_name}, url: {url}")
        response = requests.get(url)
        logging.info(f"do_custom_domain_renew_internal http check response | app_id:{app_id}, site_id:{site_id}, domain_id:{domain_id}, domain_name:{domain_name}, response: {response.text}")
        if response.text == test_value:
            logging.info(f"do_custom_domain_renew_internal http check success | app_id:{app_id}, site_id:{site_id}, domain_id:{domain_id}, domain_name:{domain_name}")
        else:
            logging.info(f"do_custom_domain_renew_internal http check failed | app_id:{app_id}, site_id:{site_id}, domain_id:{domain_id}, domain_name:{domain_name}")
            return {
                'result': 'error',
                'message': 'http_not_ready'
            }
    except Exception as e:
        logging.exception(e)
        logging.info(f"do_custom_domain_renew_internal http check exception | app_id:{app_id}, site_id:{site_id}, domain_id:{domain_id}, domain_name:{domain_name}")
        return {
            'result': 'error',
            'message': 'http_not_ready'
        }
    try:
        logging.info(f"do_custom_domain_renew_internal start cert request | app_id:{app_id}, site_id:{site_id}, domain_id:{domain_id}, domain_name:{domain_name}")
        client_acme = lanying_cert.get_acme_client()
        pkey_pem, csr_pem = lanying_cert.new_csr_comp(domain_name)
        orderr = client_acme.new_order(csr_pem)
        try:
            logging.info(f"do_custom_domain_renew_internal cert start challenge cert order | app_id:{app_id}, site_id:{site_id}, domain_id:{domain_id}, domain_name:{domain_name}")
            challb = lanying_cert.select_http01_chall(orderr)
            finalized_orderr = lanying_cert.perform_http01(client_acme, challb, orderr)
            logging.info(f"do_custom_domain_renew_internal cert finish | app_id:{app_id}, site_id:{site_id}, domain_id:{domain_id}, domain_name:{domain_name}")
            update_custom_domain_info(app_id, site_id, domain_id, "cert_pem", finalized_orderr.fullchain_pem)
            update_custom_domain_info(app_id, site_id, domain_id, "cert_key", pkey_pem)
            cert_create_time = int(time.time())
            update_custom_domain_info(app_id, site_id, domain_id, "cert_create_time", cert_create_time)
            update_custom_domain_info(app_id, site_id, domain_id, "cert_renew_failed_times", 0)
            add_custom_domain_renew_schedule(app_id, site_id, domain_id, domain_name, cert_create_time, tenement_id)
            domain_info = get_custom_domain_info(app_id, site_id, domain_id)
        except Exception as e:
            logging.exception(e)
            logging.info(f"do_custom_domain_renew_internal cert exception | app_id:{app_id}, site_id:{site_id}, domain_id:{domain_id}, domain_name:{domain_name}")
            cert_renew_failed_times = int(domain_info.get('cert_renew_failed_times', '0')) + 1
            update_custom_domain_info(app_id, site_id, domain_id, "cert_renew_failed_times", cert_renew_failed_times)
            if cert_renew_failed_times < 5:
                return {
                    'result': 'error',
                    'message': 'cert_not_ready'
                }
            else:
                return {
                    'result': 'error',
                    'message': 'cert_not_ready'
                }
    except Exception as e:
        logging.exception(e)
        logging.info(f"do_custom_domain_renew_internal cert order exception | app_id:{app_id}, site_id:{site_id}, domain_id:{domain_id}, domain_name:{domain_name}")
        return {
            'result': 'error',
            'message': 'cert_not_ready'
        }
    try:
        lanying_cdn.set_cdn_domain_cert(domain_name, domain_info['cert_pem'], domain_info['cert_key'])
        return {
            'result': 'ok'
        }
    except Exception as e:
        logging.exception(e)
        return {
            'result': 'error',
            'message': 'cert_config_not_ready'
        }

def is_maxim_top_im_gitbook(site_info):
    return 'maxim-top/im.gitbook' in site_info.get('github_url', '')

def maybe_init_analytics(app_id, site_id):
    site_info = get_site(app_id, site_id)
    if site_info:
        try:
            app_id = site_info['app_id']
            tenement_id = site_info['tenement_id']
            property_name = ''
            if 'google_analytics_property_name' not in site_info:
                logging.info(f"maybe_init_analytics create property start | app_id:{app_id}, site_id:{site_id}")
                response = lanying_google_analytics.create_properties(app_id,tenement_id,site_id)
                logging.info(f"maybe_init_analytics create property finish | app_id:{app_id}, site_id:{site_id}, response:{response}")
                if response['result'] == 'ok':
                    if 'data' in response and  'name' in response['data']:
                        logging.info(f"maybe_init_analytics update site property start | app_id:{app_id}, site_id:{site_id}")
                        property_name = response['data']['name']
                        update_site_field(app_id, site_id, 'google_analytics_property_name', property_name)
            else:
                property_name = site_info['google_analytics_property_name']
            if 'google_analytics_streams' not in site_info:
                google_analytics_streams = {}
            else:
                google_analytics_streams = lanying_utils.safe_json_loads(site_info['google_analytics_streams'])
            site_url = ''
            if 'site_url' in site_info:
                site_url = site_info['site_url']
            if 'custom_site_url' in site_info:
                site_url = site_info['custom_site_url']
            if 'canonical_link' in site_info:
                if site_info['canonical_link'] != '' and 'docs.lanyingim.com' not in site_info['canonical_link']:
                    if len(google_analytics_streams) < 8:
                        canonical_link = format_site_url(site_info['canonical_link'])
                        if canonical_link != '':
                            site_url = canonical_link
            if site_url != '':
                old_token = site_info.get('google_token','').strip()
                need_new_code = False
                if is_maxim_top_im_gitbook(site_info):
                    need_new_code = False
                elif old_token == '' or old_token == 'G-EE5J5LB4MD':
                    need_new_code = True
                else:
                    for now_site_url,stream_info in google_analytics_streams.items():
                        if stream_info['token'] == old_token and now_site_url != site_url:
                            need_new_code = True
                if need_new_code:
                    if site_url not in google_analytics_streams:
                        if property_name != '':
                            logging.info(f"maybe_init_analytics generate new stream start | app_id:{app_id}, site_id:{site_id}")
                            response = lanying_google_analytics.create_stream(app_id,tenement_id,site_id, property_name, site_url)
                            logging.info(f"maybe_init_analytics generate new stream finish | app_id:{app_id}, site_id:{site_id}, response:{response}")
                            if response['result'] == 'ok':
                                if 'data' in response and 'name' in response['data']:
                                    logging.info(f"maybe_init_analytics update site new stream start | app_id:{app_id}, site_id:{site_id}")
                                    stream_name = response['data']['name']
                                    new_token = response['data']['webStreamData']['measurementId']
                                    google_analytics_streams[site_url] = {
                                        'token': new_token,
                                        'stream_name': stream_name
                                    }
                                    new_google_analytics_streams = json.dumps(google_analytics_streams)
                                    update_site_field(app_id, site_id, 'google_token', new_token)
                                    update_site_field(app_id, site_id, 'google_analytics_streams', new_google_analytics_streams)
                    else:
                        update_site_field(app_id, site_id, 'google_token', google_analytics_streams[site_url]['token'])
        except Exception as e:
            logging.exception(e)

def format_site_url(site_url):
    if is_valid_domain(site_url):
        parse = urlparse(site_url)
        return urlunparse(parse._replace(path='/', params='',query='',fragment=''))
    else:
        return ''

def get_site_hostname(site_url):
    if is_valid_domain(site_url):
        parse = urlparse(site_url)
        return parse.hostname
    else:
        return ''

def is_valid_domain(url):
    pattern = re.compile(r'^(https?://)?([a-zA-Z0-9-]+\.)+[a-zA-Z]{2,6}(/.*)?$')
    return bool(pattern.match(url))

def get_force_content_security_site_ids(app_id):
    site_list = get_site_list(app_id)['data']['list']
    force_site_ids = set()
    for site in site_list:
        site_id = site['site_id']
        domain_id = site.get('domain_id', '')
        is_force_site_id = True
        if domain_id != '':
            domain_info = get_custom_domain_info(app_id, site_id, domain_id)
            if domain_info:
                if domain_info['state'] == 'ready':
                    if domain_info.get('cdn_status', 'online') != 'offline':
                        is_force_site_id = False
        if site.get('custom_cdn', 'off') == 'on':
            is_force_site_id = False
        if is_force_site_id:
            force_site_ids.add(site_id)
    return force_site_ids

def get_force_content_security_chatbot_ids(app_id):
    force_site_ids = get_force_content_security_site_ids(app_id)
    chatbot_ids = set()
    task_list = get_task_list(app_id)['data']['list']
    for task in task_list:
        site_id_list = task['site_id_list']
        chatbot_id = task['chatbot_id']
        if len(site_id_list) == 0:
            chatbot_ids.add(chatbot_id)
        else:
            for site_id in site_id_list:
                if site_id in force_site_ids:
                    chatbot_ids.add(chatbot_id)
    return chatbot_ids

def check_task_content_security(app_id, task_setting: TaskSetting):
    chatbot_id = task_setting.chatbot_id
    chatbot = lanying_chatbot.get_chatbot(app_id, chatbot_id)
    if chatbot is None:
        return {
            'result': 'error',
            'message': 'chatbot id not exist'
        }
    if chatbot['content_security'] == 'on':
        return {
            'result': 'ok'
        }
    force_site_ids = get_force_content_security_site_ids(app_id)
    site_id_list = task_setting.site_id_list
    if len(site_id_list) == 0:
        return {
            'result': 'error',
            'message': 'cannot bind to chatbot without content security'
        }
    else:
        for site_id in site_id_list:
            if site_id in force_site_ids:
                return {
                    'result': 'error',
                    'message': 'cannot bind to chatbot without content security'
                }
    return {
        'result': 'ok'
    }

def get_site_index_info_list():
    site_list = get_all_site_detail_list()
    dtos = []
    for site in site_list:
        dto = {}
        for field in ['app_id', 'site_id', 'name', 'site_url', 'canonical_link', 'baidu_index_pages', 'baidu_index_domain', 'baidu_index_update_time', 'google_index_pages', 'google_index_domain', 'google_index_update_time']:
            dto[field] = site[field]
        dtos.append(dto)
    return {
        'result': 'ok',
        'data': {
            'list': dtos
        }
    }

def upload_image(app_id, site_id, file_name):
    site_info = get_site(app_id, site_id)
    if site_info is None:
        return {'result': 'error', 'message': 'site not exist'}
    _,ext = os.path.splitext(file_name)
    ext = ext.lower()
    if ext not in ['.png', '.jpg', '.jpeg', '.webp']:
        return {'result': 'error', 'message': 'bad image format'}
    timestr = datetime.now().strftime('%Y%m%d%H%M%S%f')
    object_name = f'site-image/{app_id}/{site_id}/{timestr}{ext}'
    result = lanying_oss.sign_upload(object_name)
    if result['result'] == 'error':
        return result
    return {
        'result': 'ok',
        'data': result['data']
    }

def maybe_invite_github_member(app_id, old_site_info, new_site_info):
    collaborator = new_site_info['collaborator']
    old_collaborator = old_site_info.get('collaborator','')
    if new_site_info['github_hosting'] == 'on' and collaborator != old_collaborator and collaborator != '':
        site_id = new_site_info['site_id']
        result = parse_github_url(new_site_info['github_url'])
        if result['result'] == 'error':
            return result
        github_owner = result['github_owner']
        github_repo = result['github_repo']
        repo_full_name = f'{github_owner}/{github_repo}'
        g = get_github()
        repo = g.get_repo(repo_full_name)
        perrmission = 'push'
        # 发送邀请
        try:
            logging.info(f"maybe_invite_github_member invite start | app_id:{app_id}, site_id:{site_id}, username:{collaborator}, repo_full_name:{repo_full_name}")
            lanying_slack.async_send_grafana_message(f"maybe_invite_github_member invite start | app_id:{app_id}, site_id:{site_id}, username:{collaborator}, repo_full_name:{repo_full_name}")
            result = repo.add_to_collaborators(collaborator, permission=perrmission)
            logging.info(f"maybe_invite_github_member invite finish | app_id:{app_id}, site_id:{site_id}, username:{collaborator}, repo_full_name:{repo_full_name}, result:{result}")
            lanying_slack.async_send_grafana_message(f"maybe_invite_github_member invite finish | app_id:{app_id}, site_id:{site_id}, username:{collaborator}, repo_full_name:{repo_full_name}")
        except Exception as e:
            logging.exception(e)
            logging.info(f"maybe_invite_github_member invite finish | app_id:{app_id}, site_id:{site_id}, username:{collaborator}, repo_full_name:{repo_full_name}, result:exception")
            lanying_slack.async_send_grafana_message(f"maybe_invite_github_member invite finish | app_id:{app_id}, site_id:{site_id}, username:{collaborator}, repo_full_name:{repo_full_name}, result:exception")

def get_github():
    github_token = get_github_token()
    return Github(github_token)

def get_github_token():
    return os.getenv('GITHUB_HOSTING_TOKEN')

def get_grow_ai_workflow_headers():
    return {
        'Authorization': f"token {os.getenv('GROW_AI_GITHUB_TOKEN', '')}",
        'Accept': 'application/vnd.github.v3+json'
    }

def get_github_org():
    return os.getenv('GITHUB_HOSTING_ORG')

def generate_ssl_cert(app_id, domain_name):
    logging.info(f"generate_ssl_cert start | app_id:{app_id}, domain_name:{domain_name}")
    test_key = f'{app_id}_{domain_name}_{int(time.time())}'
    test_value = f'{app_id}_{domain_name}_{int(time.time())}_{random.randint(1,100000000)}'
    lanying_cert.set_acme_challenge_value(test_key,test_value)
    try:
        url = f'http://{domain_name}/.well-known/acme-challenge/{test_key}'
        logging.info(f"generate_ssl_cert http check start | app_id:{app_id}, domain_name:{domain_name}, url: {url}")
        response = requests.get(url, timeout=(10.0, 10.0))
        logging.info(f"generate_ssl_cert http check response | app_id:{app_id}, domain_name:{domain_name}, response: {response.text}")
        if response.text == test_value:
            logging.info(f"generate_ssl_cert http check success | app_id:{app_id}, domain_name:{domain_name}")
        else:
            logging.info(f"generate_ssl_cert http check failed | app_id:{app_id}, domain_name:{domain_name}")
            return {
                'result': 'error',
                'message': 'http_not_ready'
            }
    except Exception as e:
        logging.exception(e)
        logging.info(f"generate_ssl_cert http check exception | app_id:{app_id}, domain_name:{domain_name}")
        return {
            'result': 'error',
            'message': 'http_not_ready'
        }
    try:
        logging.info(f"generate_ssl_cert start cert request | app_id:{app_id}, domain_name:{domain_name}")
        client_acme = lanying_cert.get_acme_client()
        pkey_pem, csr_pem = lanying_cert.new_csr_comp(domain_name)
        orderr = client_acme.new_order(csr_pem)
        try:
            logging.info(f"generate_ssl_cert cert start challenge cert order | app_id:{app_id}, domain_name:{domain_name}")
            challb = lanying_cert.select_http01_chall(orderr)
            finalized_orderr = lanying_cert.perform_http01(client_acme, challb, orderr)
            logging.info(f"generate_ssl_cert cert finish | app_id:{app_id}, domain_name:{domain_name}")
            return {
                'result': 'ok',
                'data': {
                    'cert_pem': str(finalized_orderr.fullchain_pem),
                    'cert_key': pkey_pem.decode('utf-8')
                }
            }
        except Exception as e:
            logging.exception(e)
            logging.info(f"generate_ssl_cert cert exception | app_id:{app_id}, domain_name:{domain_name}")
            return {
                'result': 'error',
                'message': 'cert_not_ready'
            }
    except Exception as e:
        logging.exception(e)
        logging.info(f"generate_ssl_cert cert order exception | app_id:{app_id}, domain_name:{domain_name}")
        return {
            'result': 'error',
            'message': 'cert_not_ready'
        }

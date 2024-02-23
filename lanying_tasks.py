import random
import time
import os
from flask import Flask
from concurrent.futures import ThreadPoolExecutor
import lanying_config
import time
import lanying_redis
from celery import Celery
import logging
import lanying_file_storage
import lanying_embedding
import uuid
import zipfile
import lanying_url_loader
import io
import json
import lanying_ai_plugin

normal_queue = Celery('normal_queue',
             backend=lanying_redis.get_task_redis_server(),
             broker=lanying_redis.get_task_redis_server())
slow_queue = Celery('slow_queue',
             backend=lanying_redis.get_slow_task_redis_server(),
             broker=lanying_redis.get_slow_task_redis_server())
download_dir = os.getenv("EMBEDDING_DOWNLOAD_DIR", "/data/download")
embedding_doc_dir = os.getenv("EMBEDDING_DOC_DIR", "embedding-doc")
os.makedirs(download_dir, exist_ok=True)

@normal_queue.task
def add_embedding_file(trace_id, app_id, embedding_name, url, headers, origin_filename, openai_secret_key, type='file', limit=-1, opts = {}):
    storage_limit = lanying_embedding.get_app_config_int(app_id, "lanying_connector.storage_limit")
    storage_payg = lanying_embedding.get_app_config_int(app_id, "lanying_connector.storage_payg")
    logging.info(f"limit info:storage_limit:{storage_limit}, storage_payg:{storage_payg}")
    lanying_embedding.update_trace_field(trace_id, "status", "start")
    lanying_embedding.clear_trace_doc_id(trace_id)
    ext = lanying_embedding.parse_file_ext(origin_filename)
    embedding_info = lanying_embedding.get_embedding_name_info(app_id, embedding_name)
    if embedding_info is None:
        logging.info(f"embedding_name not exist: trace_id={trace_id}, app_id={app_id}, embedding_name={embedding_name}")
        lanying_embedding.update_trace_field(trace_id, "status", "error")
        lanying_embedding.update_trace_field(trace_id, "message", "embedding_name not exist")
        return
    embedding_uuid = embedding_info["embedding_uuid"]
    tasks = []
    embedding_uuid_info = lanying_embedding.get_embedding_uuid_info(embedding_uuid)
    vendor = embedding_uuid_info.get('vendor', 'openai')
    lanying_embedding.update_embedding_uuid_info(embedding_uuid, "openai_secret_key", openai_secret_key)
    if type not in  ['site', 'task']:
        temp_filename = os.path.join(f"{download_dir}/{app_id}-{embedding_uuid}-{uuid.uuid4()}{ext}")
        download_result = lanying_file_storage.download_url(url, {} if type == 'url' else headers, temp_filename)
        if download_result["result"] == "error":
            lanying_embedding.update_trace_field(trace_id, "status", "error")
            lanying_embedding.update_trace_field(trace_id, "message", "download embedding file error")
            doc_id = lanying_embedding.generate_doc_id(embedding_uuid)
            lanying_embedding.create_doc_info(app_id, embedding_uuid, url if type == 'url' else origin_filename, '', doc_id, 0, ext, type, url, vendor,opts)
            lanying_embedding.add_doc_to_embedding(embedding_uuid, doc_id)
            lanying_embedding.update_doc_field(embedding_uuid, doc_id, "status", "error")
            lanying_embedding.update_doc_field(embedding_uuid, doc_id, "reason", "download_failed")
            logging.error(f"fail to download embedding: trace_id={trace_id}, app_id={app_id}, embedding_name={embedding_name}, embedding_uuid={embedding_uuid}, url={url}, message:{download_result['message']}")
            lanying_embedding.trace_finish(trace_id, app_id, "error", "download failed", doc_id, embedding_name)
            return
        if ext in [".zip"]:
            with zipfile.ZipFile(temp_filename, 'r') as zip_ref:
                sub_filenames = zip_ref.namelist()
                logging.info(f"add_embedding_file | got zip filenames: app_id={app_id}, embedding_name={embedding_name}, embedding_uuid={embedding_uuid}, sub_filenames:{sub_filenames}")
                for sub_filename in sub_filenames:
                    sub_ext = lanying_embedding.parse_file_ext(sub_filename)
                    try:
                        right_filename = sub_filename.encode('cp437').decode('utf-8')
                    except Exception as e:
                        try:
                            right_filename = sub_filename.encode('cp437').decode('gbk')
                        except Exception as ee:
                            right_filename = sub_filename
                    if "__MACOSX" in sub_filename:
                        pass
                    elif "DS_Store" in sub_filename:
                        pass
                    elif sub_ext in lanying_embedding.allow_exts():
                        logging.info(f"add_embedding_file | start process sub file: sub_filename:{sub_filename}, right_filename:{right_filename}")
                        sub_file_info = zip_ref.getinfo(sub_filename)
                        sub_file_size = sub_file_info.file_size
                        doc_id = lanying_embedding.generate_doc_id(embedding_uuid)
                        object_name = os.path.join(f"{embedding_doc_dir}/{app_id}/{embedding_uuid}/{doc_id}{sub_ext}")
                        lanying_embedding.create_doc_info(app_id, embedding_uuid, right_filename, object_name, doc_id, sub_file_size, sub_ext, type, url, vendor,opts)
                        lanying_embedding.add_trace_doc_id(trace_id, doc_id)
                        with zip_ref.open(sub_filename) as sub_file_ref:
                            upload_result = lanying_file_storage.put_object(object_name, sub_file_ref, sub_file_size)
                            if upload_result["result"] == "error":
                                lanying_embedding.update_trace_field(trace_id, "status", "error")
                                lanying_embedding.update_trace_field(trace_id, "message", "upload embedding sub file error")
                                task_error(f"fail to upload sub file : app_id={app_id}, embedding_name={embedding_name}, embedding_uuid={embedding_uuid},object_name:{object_name}")
                            tasks.append((object_name, origin_filename, doc_id))
                    else:
                        sub_file_info = zip_ref.getinfo(sub_filename)
                        if sub_file_info.is_dir():
                            pass
                        else:
                            sub_file_size = sub_file_info.file_size
                            logging.info(f"add_embedding_file | skip process sub file: sub_filename:{sub_filename}, right_filename:{right_filename}")
                            doc_id = lanying_embedding.generate_doc_id(embedding_uuid)
                            lanying_embedding.create_doc_info(app_id, embedding_uuid, right_filename, '', doc_id, sub_file_size, sub_ext, type, url, vendor,opts)
                            lanying_embedding.add_doc_to_embedding(embedding_uuid, doc_id)
                            lanying_embedding.update_doc_field(embedding_uuid, doc_id, "status", "error")
                            lanying_embedding.update_doc_field(embedding_uuid, doc_id, "reason", "bad_ext")
        elif ext in lanying_embedding.allow_exts():
            file_stat = os.stat(temp_filename)
            file_size = file_stat.st_size
            doc_id = lanying_embedding.generate_doc_id(embedding_uuid)
            object_name = os.path.join(f"{embedding_doc_dir}/{app_id}/{embedding_uuid}/{doc_id}{ext}")
            lanying_embedding.create_doc_info(app_id, embedding_uuid, url if type == 'url' else origin_filename, object_name, doc_id, file_size, ext, type, url, vendor,opts)
            lanying_embedding.add_trace_doc_id(trace_id, doc_id)
            if 'metadata' in opts:
                lanying_embedding.set_doc_metadata(app_id, embedding_name, doc_id, opts['metadata'])
            upload_result = lanying_file_storage.upload(object_name, temp_filename)
            if upload_result["result"] == "error":
                lanying_embedding.update_trace_field(trace_id, "status", "error")
                lanying_embedding.update_trace_field(trace_id, "message", "upload embedding file error")
                lanying_embedding.trace_finish(trace_id, app_id, "error", "upload embedding file error", doc_id, embedding_name)
                task_error(f"fail to upload embedding: app_id={app_id}, embedding_name={embedding_name}, embedding_uuid={embedding_uuid}, url={url}, message:{upload_result['message']}")
            tasks.append((object_name, origin_filename, doc_id))
    for now_object_name, now_origin_filename, now_doc_id in tasks:
        lanying_embedding.add_doc_to_embedding(embedding_uuid, now_doc_id)
        process_embedding_file.apply_async(args = [trace_id, app_id, embedding_uuid, now_object_name, now_origin_filename, now_doc_id, False])

@slow_queue.task
def load_site(trace_id, app_id, embedding_uuid, ext, type, site_task_id, url, doc_cnt, limit):
    tasks = []
    ttl = 50
    is_finish = True
    embedding_uuid_info = lanying_embedding.get_embedding_uuid_info(embedding_uuid)
    if embedding_uuid_info:
        vendor = embedding_uuid_info.get('vendor', 'openai')
        for doc in lanying_url_loader.do_task(site_task_id, url):
            is_finish = False
            if doc:
                try:
                    source = doc.metadata['source']
                    page_bytes = doc.metadata['page_bytes']
                    logging.info(f"load_site found url: app_id={app_id}, embedding_uuid={embedding_uuid},url:{source}, len:{len(page_bytes)}")
                    doc_id = lanying_embedding.generate_doc_id(embedding_uuid)
                    object_name = os.path.join(f"{embedding_doc_dir}/{app_id}/{embedding_uuid}/{doc_id}{ext}")
                    lanying_embedding.create_doc_info(app_id, embedding_uuid, source, object_name, doc_id, len(page_bytes), ext, type, url, vendor,{})
                    lanying_embedding.add_trace_doc_id(trace_id, doc_id)
                    upload_result = lanying_file_storage.put_object(object_name, io.BytesIO(page_bytes), len(page_bytes))
                    if upload_result["result"] == "error":
                        lanying_embedding.update_trace_field(trace_id, "status", "error")
                        lanying_embedding.update_trace_field(trace_id, "message", "upload embedding sub file error")
                        task_error(f"fail to upload sub file : app_id={app_id}, embedding_uuid={embedding_uuid},object_name:{object_name},source:{source}")
                    tasks.append((object_name, source, doc_id))
                    lanying_embedding.add_doc_to_embedding(embedding_uuid, doc_id)
                    process_embedding_file.apply_async(args = [trace_id, app_id, embedding_uuid, object_name, source, doc_id, False])
                except Exception as e:
                    logging.exception(e)
                doc_cnt += 1
            ttl -= 1
            if limit > 0 and doc_cnt > limit:
                is_finish = True
                break
            if ttl < 0:
                break
        if not is_finish:
            load_site.apply_async(args = [trace_id, app_id, embedding_uuid, ext, type, site_task_id, url, doc_cnt, limit])
        else:
            lanying_url_loader.clean_task(site_task_id)

@slow_queue.task
def continue_site_task(trace_id, app_id, embedding_uuid, task_id):
    ext = '.html'
    type = 'site'
    embedding_uuid_info = lanying_embedding.get_embedding_uuid_info(embedding_uuid)
    if embedding_uuid_info:
        vendor = embedding_uuid_info.get('vendor', 'openai')
        task_info = lanying_embedding.get_task(embedding_uuid, task_id)
        if task_info:
            task_url = task_info['url']
            urls = []
            ttl = 20
            is_finish = True
            generate_lanying_links = task_info.get('generate_lanying_links', 'False') == 'True'
            opts = {'generate_lanying_links': generate_lanying_links}
            for url,_ in lanying_embedding.get_task_details_iterator(embedding_uuid, task_id):
                ttl -= 1
                urls.append(url)
                logging.info(f"continue_site_task | processing url:{url}, trace_id:{trace_id}, app_id:{app_id}, embedding_uuid:{embedding_uuid}, task_id:{task_id}")
                temp_filename = os.path.join(f"{download_dir}/{app_id}-{embedding_uuid}-{uuid.uuid4()}{ext}")
                download_result = lanying_file_storage.download_url(url, {}, temp_filename)
                if download_result["result"] == "error":
                    lanying_embedding.update_trace_field(trace_id, "status", "error")
                    lanying_embedding.update_trace_field(trace_id, "message", "download embedding file error")
                    doc_id = lanying_embedding.generate_doc_id(embedding_uuid)
                    lanying_embedding.create_doc_info(app_id, embedding_uuid, url, '', doc_id, 0, ext, type, task_url, vendor, opts)
                    lanying_embedding.update_doc_field(embedding_uuid,doc_id, "task_id", task_id)
                    lanying_embedding.add_doc_to_embedding(embedding_uuid, doc_id)
                    lanying_embedding.update_doc_field(embedding_uuid, doc_id, "status", "error")
                    lanying_embedding.update_doc_field(embedding_uuid, doc_id, "reason", "download_failed")
                    lanying_embedding.increase_task_field(embedding_uuid, task_id, "processing_fail_num", 1)
                    logging.error(f"fail to download embedding: trace_id={trace_id}, app_id={app_id}, embedding_uuid={embedding_uuid}, url={url}, task_id:{task_id}, message:{download_result['message']}")
                    continue
                file_stat = os.stat(temp_filename)
                file_size = file_stat.st_size
                doc_id = lanying_embedding.generate_doc_id(embedding_uuid)
                object_name = os.path.join(f"{embedding_doc_dir}/{app_id}/{embedding_uuid}/{doc_id}{ext}")
                lanying_embedding.create_doc_info(app_id, embedding_uuid, url, object_name, doc_id, file_size, ext, type, url, vendor, opts)
                lanying_embedding.update_doc_field(embedding_uuid,doc_id, "task_id", task_id)
                lanying_embedding.add_trace_doc_id(trace_id, doc_id)
                upload_result = lanying_file_storage.upload(object_name, temp_filename)
                if upload_result["result"] == "error":
                    lanying_embedding.update_trace_field(trace_id, "status", "error")
                    lanying_embedding.update_trace_field(trace_id, "message", "upload embedding file error")
                    lanying_embedding.increase_task_field(embedding_uuid, task_id, "processing_fail_num", 1)
                    logging.error(f"fail to upload embedding: app_id={app_id}, embedding_uuid={embedding_uuid}, url={url}, task_id:{task_id}, message:{upload_result['message']}")
                    continue
                lanying_embedding.add_doc_to_embedding(embedding_uuid, doc_id)
                process_embedding_file_slow.apply_async(args = [trace_id, app_id, embedding_uuid, object_name, url, doc_id, False])
                if ttl <= 0:
                    is_finish = False
                    break
            lanying_embedding.delete_task_details_by_fields(embedding_uuid, task_id, urls)
            if not is_finish:
                logging.info(f"continue_site_task | schedule continue,  trace_id:{trace_id}, app_id:{app_id}, embedding_uuid:{embedding_uuid}, task_id:{task_id}")
                continue_site_task.apply_async(args = [trace_id, app_id, embedding_uuid, task_id])
            else:
                logging.info(f"continue_site_task | finish, trace_id:{trace_id}, app_id:{app_id}, embedding_uuid:{embedding_uuid}, task_id:{task_id}")


@slow_queue.task
def prepare_site(trace_id, app_id, embedding_uuid, ext, type, site_task_id, urls, doc_cnt, limit, task_id, max_depth, filters, opts={}):
    task_info = lanying_embedding.get_task(embedding_uuid, task_id)
    if task_info is None:
        lanying_url_loader.clean_task(site_task_id)
        return
    if task_info["status"] == "wait":
        lanying_embedding.update_task_field(embedding_uuid, task_id, "status", "processing")
    elif task_info["status"] == "processing":
        pass
    else:
        logging.info(f"prepare_site | bad task_status: {task_info['status']}, app_id={app_id}, embedding_uuid={embedding_uuid}, site_task_id:{site_task_id}, task_id:{site_task_id}")
        return
    ttl = 50
    is_finish = True
    for doc in lanying_url_loader.do_task(site_task_id, urls, max_depth, filters):
        is_finish = False
        if doc:
            try:
                task_info = lanying_embedding.get_task(embedding_uuid, task_id)
                if task_info is None:
                    return
                source = doc.metadata['source']
                page_bytes = doc.metadata['page_bytes']
                logging.info(f"prepare_site found url: app_id={app_id}, embedding_uuid={embedding_uuid},url:{source}, len:{len(page_bytes)}, site_task_id:{site_task_id}, task_id:{site_task_id}")
                block_num = lanying_embedding.estimate_html(embedding_uuid, page_bytes.decode("utf-8"))
                file_size = len(page_bytes)
                if lanying_embedding.get_task_detail_field(embedding_uuid, task_id, source) is None:
                    field_value = {'file_size':file_size, 'block_num': block_num}
                    lanying_embedding.set_task_detail_field(embedding_uuid,task_id, source, json.dumps(field_value, ensure_ascii=False))
                    lanying_embedding.increase_task_field(embedding_uuid, task_id, "block_num", block_num)
                    lanying_embedding.increase_task_field(embedding_uuid, task_id, "file_size", file_size)
                    lanying_embedding.increase_task_field(embedding_uuid, task_id, "found_num", 1)
            except Exception as e:
                logging.exception(e)
            doc_cnt += 1
        lanying_embedding.increase_task_field(embedding_uuid, task_id, "visited_num", 1)
        ttl -= 1
        if limit > 0 and doc_cnt > limit:
            is_finish = True
            break
        if ttl < 0:
            break
    if not is_finish:
        prepare_site.apply_async(args = [trace_id, app_id, embedding_uuid, ext, type, site_task_id, urls, doc_cnt, limit, task_id, max_depth, filters, opts])
    else:
        lanying_embedding.update_task_field(embedding_uuid, task_id, "status", "finish")

@normal_queue.task
def process_embedding_file(trace_id, app_id, embedding_uuid, object_name, origin_filename, doc_id, is_regenerate = False):
    process_embedding_file_internal(trace_id, app_id, embedding_uuid, object_name, origin_filename, doc_id, is_regenerate)

@slow_queue.task
def process_embedding_file_slow(trace_id, app_id, embedding_uuid, object_name, origin_filename, doc_id, is_regenerate = False):
    process_embedding_file_internal(trace_id, app_id, embedding_uuid, object_name, origin_filename, doc_id, is_regenerate)

def process_embedding_file_internal(trace_id, app_id, embedding_uuid, object_name, origin_filename, doc_id, is_regenerate = False):
    embedding_uuid_info = lanying_embedding.get_embedding_uuid_info(embedding_uuid)
    if embedding_uuid_info is None:
        logging.info(f"process_embedding_file skip for not_found embedding_uuid_info | embedding_uuid:{embedding_uuid}")
        return
    embedding_name = embedding_uuid_info['embedding_name']
    doc_info = lanying_embedding.get_doc(embedding_uuid, doc_id)
    if doc_info is None:
        return
    is_storage_size_increased = False
    doc_task_id = None
    if not is_regenerate:
        if 'task_id' in doc_info:
            task_id = doc_info['task_id']
            task_info = lanying_embedding.get_task(embedding_uuid, task_id)
            if task_info:
                doc_task_id = task_id
    try:
        storage_file_size = int(doc_info.get('storage_file_size', "0"))
        lanying_embedding.update_doc_field(embedding_uuid, doc_id, "status", "processing")
        lanying_embedding.update_doc_field(embedding_uuid, doc_id, "max_block_size", embedding_uuid_info['max_block_size'])
        if 'ext' in doc_info:
            ext = doc_info['ext']
        else: # for old
            ext = lanying_embedding.parse_file_ext(origin_filename)
        if 'type' in doc_info:
            is_file_url = doc_info['type'] in ["site", "url"]
        else:
            is_file_url = lanying_embedding.is_file_url(origin_filename)
        temp_filename = os.path.join(f"{download_dir}/sub-{app_id}-{embedding_uuid}-{uuid.uuid4()}{ext}")
        if is_regenerate and is_file_url:
            if ext == '.txt':
                download_result = lanying_file_storage.download_url_in_text_format(origin_filename, temp_filename)
            else:
                download_result = lanying_file_storage.download_url(origin_filename, {}, temp_filename)
            logging.info(f"re download from url | app_id={app_id}, doc_id={doc_id}, result={download_result}, ext={ext}")
            if download_result["result"] == "error" and object_name != '':
                download_result = lanying_file_storage.download(object_name, temp_filename)
        else:
            download_result = lanying_file_storage.download(object_name, temp_filename)
        if download_result["result"] == "error":
            lanying_embedding.update_doc_field(embedding_uuid, doc_id, "status", "error")
            lanying_embedding.update_doc_field(embedding_uuid, doc_id, "reason", "download_error")
            if doc_task_id:
                lanying_embedding.increase_task_field(embedding_uuid, doc_task_id, "processing_fail_num", 1)
                maybe_task_finish(embedding_uuid, doc_task_id)
            logging.info(f"fail to download embedding: trace_id={trace_id}, app_id={app_id}, embedding_name={embedding_uuid}, object_name={object_name},doc_id:{doc_id}, message:{download_result['message']}")
            lanying_embedding.trace_finish(trace_id, app_id, "error", "download file error", doc_id, embedding_name)
            return
        old_file_size = int(doc_info["file_size"])
        file_stat = os.stat(temp_filename)
        file_size = file_stat.st_size
        if file_size != old_file_size:
            lanying_embedding.update_doc_field(embedding_uuid, doc_id, "file_size", file_size)
        delta_file_size = file_size - storage_file_size
        result = lanying_embedding.add_storage_size(app_id, embedding_uuid, doc_id, delta_file_size)
        if result["result"] == "error":
            logging.info(f"process_embedding_file | reach storage limit, app_id:{app_id}, embedding_uuid:{embedding_uuid},doc_id:{doc_id}")
            lanying_embedding.update_doc_field(embedding_uuid, doc_id, "status", "error")
            lanying_embedding.update_doc_field(embedding_uuid, doc_id, "reason", "storage_limit")
            if doc_task_id:
                lanying_embedding.increase_task_field(embedding_uuid, doc_task_id, "processing_fail_num", 1)
                maybe_task_finish(embedding_uuid, doc_task_id)
            lanying_embedding.trace_finish(trace_id, app_id, "error", "storage limit", doc_id, embedding_name)
            return
        is_storage_size_increased = True
        if is_regenerate:
            redis = lanying_redis.get_redis_stack_connection()
            lanying_embedding.update_doc_field(embedding_uuid, doc_id, "block_id_seq", 0)
            lanying_embedding.increase_embedding_uuid_field(redis, embedding_uuid, "embedding_count", -int(doc_info.get("embedding_count", "0")))
            lanying_embedding.increase_embedding_doc_field(redis, embedding_uuid, doc_id, "embedding_count", -int(doc_info.get("embedding_count", "0")))
            lanying_embedding.increase_embedding_uuid_field(redis, embedding_uuid, "embedding_size", -int(doc_info.get("embedding_size", "0")))
            lanying_embedding.increase_embedding_doc_field(redis, embedding_uuid, doc_id, "embedding_size", -int(doc_info.get("embedding_size", "0")))
            lanying_embedding.increase_embedding_uuid_field(redis, embedding_uuid, "text_size", -int(doc_info.get("text_size", "0")))
            lanying_embedding.increase_embedding_doc_field(redis, embedding_uuid, doc_id, "text_size", -int(doc_info.get("text_size", "0")))
            lanying_embedding.increase_embedding_uuid_field(redis, embedding_uuid, "token_cnt", -int(doc_info.get("token_cnt", "0")))
            lanying_embedding.increase_embedding_doc_field(redis, embedding_uuid, doc_id, "token_cnt", -int(doc_info.get("token_cnt", "0")))
            lanying_embedding.increase_embedding_uuid_field(redis, embedding_uuid, "char_cnt", -int(doc_info.get("char_cnt", "0")))
            lanying_embedding.increase_embedding_doc_field(redis, embedding_uuid, doc_id, "char_cnt", -int(doc_info.get("char_cnt", "0")))
            embedding_name = embedding_uuid_info["embedding_name"]
            embedding_index = lanying_embedding.get_embedding_index(app_id, embedding_name)
            if embedding_index:
                logging.info(f"start clean old doc data | app_id={app_id}, doc_id:{doc_id}")
                db_type = embedding_uuid_info.get('db_type', 'redis')
                db_table_name = embedding_uuid_info.get('db_table_name', '')
                delete_doc_data(app_id, embedding_name, doc_id, embedding_index, 0, db_type, db_table_name)
                logging.info(f"finish clean old doc data | app_id={app_id}, doc_id:{doc_id}")
        lanying_embedding.process_embedding_file(trace_id, app_id, embedding_uuid, temp_filename, origin_filename, doc_id, ext)
        if doc_task_id:
            lanying_embedding.increase_task_field(embedding_uuid, doc_task_id, "processing_success_num", 1)
            maybe_task_finish(embedding_uuid, doc_task_id)
        lanying_embedding.trace_finish(trace_id, app_id, "success", "success", doc_id, embedding_name)
    except Exception as e:
        reason = "exception"
        try:
            exception_str = str(e)
            if exception_str in ["bad_authorization","no_quota", "deduct_failed"]:
                reason = exception_str
        except Exception as ee:
            pass
        lanying_embedding.update_doc_field(embedding_uuid, doc_id, "status", "error")
        lanying_embedding.update_doc_field(embedding_uuid, doc_id, "reason", reason)
        if is_storage_size_increased:
            lanying_embedding.restore_storage_size(app_id, embedding_uuid, doc_id)
        if doc_task_id:
            lanying_embedding.increase_task_field(embedding_uuid, doc_task_id, "processing_fail_num", 1)
            maybe_task_finish(embedding_uuid, doc_task_id)
        logging.error(f"fail to process embedding file:trace_id={trace_id}, app_id={app_id}, embedding_uuid={embedding_uuid}, object_name={object_name},doc_id:{doc_id}")
        lanying_embedding.trace_finish(trace_id, app_id, "error", reason, doc_id, embedding_name)
        raise e

def maybe_task_finish(embedding_uuid, task_id):
    task_info = lanying_embedding.get_task(embedding_uuid, task_id)
    if task_info:
        if task_info["status"] == "adding":
            processing_success_num = int(task_info.get("processing_success_num", "0"))
            processing_fail_num = int(task_info.get("processing_fail_num", "0"))
            processing_total_num = int(task_info.get("processing_total_num","0"))
            if processing_fail_num + processing_success_num == processing_total_num:
                lanying_embedding.update_task_field(embedding_uuid, task_id, "status", "add_finish")

@normal_queue.task
def re_run_doc_to_embedding_by_doc_ids(trace_id, app_id, embedding_uuid, doc_ids):
    count = len(doc_ids)
    for doc_id in doc_ids:
        try:
            doc_info = lanying_embedding.get_doc(embedding_uuid, doc_id)
            if doc_info:
                lanying_embedding.update_doc_field(embedding_uuid, doc_id, "status", "wait")
                lanying_embedding.update_doc_field(embedding_uuid, doc_id, "update_time", int(time.time()))
                lanying_embedding.update_doc_field(embedding_uuid, doc_id, "progress_total", 0)
                lanying_embedding.update_doc_field(embedding_uuid, doc_id, "progress_finish", 0)
                if count > 100:
                    process_embedding_file_slow.apply_async(args = [trace_id, app_id, embedding_uuid, doc_info['object_name'], doc_info['filename'], doc_id, True])
                else:
                    process_embedding_file.apply_async(args = [trace_id, app_id, embedding_uuid, doc_info['object_name'], doc_info['filename'], doc_id, True])
        except Exception as e:
            logging.exception(e)

@normal_queue.task
def delete_doc_data(app_id, embedding_name, doc_id, embedding_index, last_total, db_type, db_table_name):
    lanying_embedding.search_doc_data_and_delete(app_id, embedding_name, doc_id, embedding_index, last_total, db_type, db_table_name)

@normal_queue.task
def process_function_embeddings(app_id, plugin_id, function_ids):
    for function_id in function_ids:
        lanying_ai_plugin.process_function_embedding(app_id, plugin_id, function_id)

def task_error(message):
    logging.error(message)
    raise Exception(message)

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
from langchain.document_loaders.recursive_url_loader import RecursiveUrlLoader
import io

app = Celery('lanying-connector',
             backend=lanying_redis.get_task_redis_server(),
             broker=lanying_redis.get_task_redis_server())
download_dir = os.getenv("EMBEDDING_DOWNLOAD_DIR", "/data/download")
embedding_doc_dir = os.getenv("EMBEDDING_DOC_DIR", "embedding-doc")
os.makedirs(download_dir, exist_ok=True)

@app.task
def add_embedding_file(trace_id, app_id, embedding_name, url, headers, origin_filename, openai_secret_key, type='file', limit=-1):
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
    lanying_embedding.update_embedding_uuid_info(embedding_uuid, "openai_secret_key", openai_secret_key)
    if type == 'site':
        loader = RecursiveUrlLoader(url=url)
        doc_cnt = 0
        for doc in loader.lazy_load():
            doc_cnt += 1
            if limit > 0 and doc_cnt > limit:
                break
            try:
                source = doc.metadata['source']
                content = lanying_embedding.remove_space_line(doc.page_content).encode('utf-8')
                if len(source) > 0 and len(content) > 0:
                    doc_id = lanying_embedding.generate_doc_id(embedding_uuid)
                    object_name = os.path.join(f"{embedding_doc_dir}/{app_id}/{embedding_uuid}/{doc_id}{ext}")
                    lanying_embedding.create_doc_info(embedding_uuid, source, object_name, doc_id, len(content), ext, type, url)
                    lanying_embedding.add_trace_doc_id(trace_id, doc_id)
                    upload_result = lanying_file_storage.put_object(object_name, io.BytesIO(content), len(content))
                    if upload_result["result"] == "error":
                        lanying_embedding.update_trace_field(trace_id, "status", "error")
                        lanying_embedding.update_trace_field(trace_id, "message", "upload embedding sub file error")
                        task_error(f"fail to upload sub file : app_id={app_id}, embedding_name={embedding_name}, embedding_uuid={embedding_uuid},object_name:{object_name},source:{source}")
                    tasks.append((object_name, source, doc_id))
            except Exception as e:
                logging.exception(e)
    else:
        temp_filename = os.path.join(f"{download_dir}/{app_id}-{embedding_uuid}-{uuid.uuid4()}{ext}")
        download_result = lanying_file_storage.download_url(url, {} if type == 'url' else headers, temp_filename)
        if download_result["result"] == "error":
            lanying_embedding.update_trace_field(trace_id, "status", "error")
            lanying_embedding.update_trace_field(trace_id, "message", "download embedding file error")
            doc_id = lanying_embedding.generate_doc_id(embedding_uuid)
            lanying_embedding.create_doc_info(embedding_uuid, url if type == 'url' else origin_filename, '', doc_id, 0, ext, type, url)
            lanying_embedding.add_doc_to_embedding(embedding_uuid, doc_id)
            lanying_embedding.update_doc_field(embedding_uuid, doc_id, "status", "error")
            lanying_embedding.update_doc_field(embedding_uuid, doc_id, "reason", "download_failed")
            logging.error(f"fail to download embedding: trace_id={trace_id}, app_id={app_id}, embedding_name={embedding_name}, embedding_uuid={embedding_uuid}, url={url}, message:{download_result['message']}")
            return
        if ext in [".zip"]:
            with zipfile.ZipFile(temp_filename, 'r') as zip_ref:
                sub_filenames = zip_ref.namelist()
                logging.info(f"add_embedding_file | got zip filenames: app_id={app_id}, embedding_name={embedding_name}, embedding_uuid={embedding_uuid}, sub_filenames:{sub_filenames}")
                for sub_filename in sub_filenames:
                    sub_ext = lanying_embedding.parse_file_ext(sub_filename)
                    right_filename = sub_filename.encode('cp437').decode('utf-8')
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
                        lanying_embedding.create_doc_info(embedding_uuid, right_filename, object_name, doc_id, sub_file_size, sub_ext, type, url)
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
                            lanying_embedding.create_doc_info(embedding_uuid, right_filename, '', doc_id, sub_file_size, sub_ext, type, url)
                            lanying_embedding.add_doc_to_embedding(embedding_uuid, doc_id)
                            lanying_embedding.update_doc_field(embedding_uuid, doc_id, "status", "error")
                            lanying_embedding.update_doc_field(embedding_uuid, doc_id, "reason", "bad_ext")
        elif ext in lanying_embedding.allow_exts():
            file_stat = os.stat(temp_filename)
            file_size = file_stat.st_size
            doc_id = lanying_embedding.generate_doc_id(embedding_uuid)
            object_name = os.path.join(f"{embedding_doc_dir}/{app_id}/{embedding_uuid}/{doc_id}{ext}")
            lanying_embedding.create_doc_info(embedding_uuid, url if type == 'url' else origin_filename, object_name, doc_id, file_size, ext, type, url)
            lanying_embedding.add_trace_doc_id(trace_id, doc_id)
            upload_result = lanying_file_storage.upload(object_name, temp_filename)
            if upload_result["result"] == "error":
                lanying_embedding.update_trace_field(trace_id, "status", "error")
                lanying_embedding.update_trace_field(trace_id, "message", "upload embedding file error")
                task_error(f"fail to upload embedding: app_id={app_id}, embedding_name={embedding_name}, embedding_uuid={embedding_uuid}, url={url}, message:{upload_result['message']}")
            tasks.append((object_name, origin_filename, doc_id))
    for now_object_name, now_origin_filename, now_doc_id in tasks:
        lanying_embedding.add_doc_to_embedding(embedding_uuid, now_doc_id)
        process_embedding_file.apply_async(args = [trace_id, app_id, embedding_uuid, now_object_name, now_origin_filename, now_doc_id, False])

@app.task
def process_embedding_file(trace_id, app_id, embedding_uuid, object_name, origin_filename, doc_id, is_regenerate = False):
    embedding_uuid_info = lanying_embedding.get_embedding_uuid_info(embedding_uuid)
    if embedding_uuid_info is None:
        logging.info(f"process_embedding_file skip for not_found embedding_uuid_info | embedding_uuid:{embedding_uuid}")
        return
    doc_info = lanying_embedding.get_doc(embedding_uuid, doc_id)
    if doc_info is None:
        return
    is_storage_size_increased = False
    try:
        storage_file_size = int(doc_info.get('storage_file_size', "0"))
        lanying_embedding.update_doc_field(embedding_uuid, doc_id, "status", "processing")
        lanying_embedding.update_doc_field(embedding_uuid, doc_id, "max_block_size", embedding_uuid_info['max_block_size'])
        if 'ext' in doc_info:
            ext = doc_info['ext']
        else: # for old
            ext = lanying_embedding.parse_file_ext(origin_filename)
        if 'type' in doc_info:
            is_file_url = doc_info['source'] in ["site", "url"]
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
            task_error(f"fail to download embedding: trace_id={trace_id}, app_id={app_id}, embedding_name={embedding_uuid}, object_name={object_name},doc_id:{doc_id}, message:{download_result['message']}")
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
                delete_doc_data(app_id, embedding_name, doc_id, embedding_index, 0)
                logging.info(f"finish clean old doc data | app_id={app_id}, doc_id:{doc_id}")
        lanying_embedding.process_embedding_file(trace_id, app_id, embedding_uuid, temp_filename, origin_filename, doc_id, ext)
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
        logging.error(f"fail to process embedding file:trace_id={trace_id}, app_id={app_id}, embedding_uuid={embedding_uuid}, object_name={object_name},doc_id:{doc_id}")
        raise e

@app.task
def re_run_doc_to_embedding_by_doc_ids(trace_id, app_id, embedding_uuid, doc_ids):
    for doc_id in doc_ids:
        try:
            doc_info = lanying_embedding.get_doc(embedding_uuid, doc_id)
            if doc_info:
                lanying_embedding.update_doc_field(embedding_uuid, doc_id, "status", "wait")
                lanying_embedding.update_doc_field(embedding_uuid, doc_id, "update_time", int(time.time()))
                lanying_embedding.update_doc_field(embedding_uuid, doc_id, "progress_total", 0)
                lanying_embedding.update_doc_field(embedding_uuid, doc_id, "progress_finish", 0)
                process_embedding_file.apply_async(args = [trace_id, app_id, embedding_uuid, doc_info['object_name'], doc_info['filename'], doc_id, True])
        except Exception as e:
            logging.exception(e)

@app.task
def delete_doc_data(app_id, embedding_name, doc_id, embedding_index, last_total):
    lanying_embedding.search_doc_data_and_delete(app_id, embedding_name, doc_id, embedding_index, last_total)

def task_error(message):
    logging.error(message)
    raise Exception(message)

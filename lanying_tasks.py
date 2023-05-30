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

app = Celery('lanying-connector',
             backend=lanying_redis.get_task_redis_server(),
             broker=lanying_redis.get_task_redis_server())
download_dir = os.getenv("EMBEDDING_DOWNLOAD_DIR", "/data/download")
embedding_doc_dir = os.getenv("EMBEDDING_DOC_DIR", "embedding-doc")
os.makedirs(download_dir, exist_ok=True)

@app.task
def add_embedding_file(trace_id, app_id, embedding_name, url, headers, origin_filename, openai_secret_key):
    lanying_embedding.update_trace_field(trace_id, "status", "start")
    lanying_embedding.clear_trace_doc_id(trace_id)
    _,ext = os.path.splitext(origin_filename)
    embedding_info = lanying_embedding.get_embedding_info(app_id, embedding_name)
    if "embedding_uuid" not in embedding_info:
        logging.info(f"embedding_name not exist: trace_id={trace_id}, app_id={app_id}, embedding_name={embedding_name}")
        lanying_embedding.update_trace_field(trace_id, "status", "error")
        lanying_embedding.update_trace_field(trace_id, "message", "embedding_name not exist")
        return
    embedding_uuid = embedding_info["embedding_uuid"]
    lanying_embedding.update_embedding_uuid_info(embedding_uuid, "openai_secret_key", openai_secret_key)
    temp_filename = os.path.join(f"{download_dir}/{app_id}-{embedding_uuid}-{uuid.uuid4()}{ext}")
    download_result = lanying_file_storage.download_url(url, headers, temp_filename)
    tasks = []
    if download_result["result"] == "error":
        lanying_embedding.update_trace_field(trace_id, "status", "error")
        lanying_embedding.update_trace_field(trace_id, "message", "download embedding file error")
        task_error(f"fail to download embedding: trace_id={trace_id}, app_id={app_id}, embedding_name={embedding_name}, embedding_uuid={embedding_uuid}, url={url}, message:{download_result['message']}")
    if ext in [".zip"]:
        with zipfile.ZipFile(temp_filename, 'r') as zip_ref:
            sub_filenames = zip_ref.namelist()
            logging.debug(f"add_embedding_file | got zip filenames: app_id={app_id}, embedding_name={embedding_name}, embedding_uuid={embedding_uuid}, sub_filenames:{sub_filenames}")
            for sub_filename in sub_filenames:
                _,sub_ext = os.path.splitext(sub_filename)
                if sub_ext in [".html", ".htm", ".csv"]:
                    logging.debug("add_embedding_file | start process sub file: sub_filenames:{sub_filenames}")
                    sub_file_info = zip_ref.getinfo(sub_filename)
                    sub_file_size = sub_file_info.file_size
                    doc_id = lanying_embedding.generate_doc_id(embedding_uuid)
                    object_name = os.path.join(f"{embedding_doc_dir}/{app_id}/{embedding_uuid}/{doc_id}{ext}")
                    lanying_embedding.create_doc_info(embedding_uuid, sub_filename, object_name, doc_id, sub_file_size)
                    lanying_embedding.add_trace_doc_id(trace_id, doc_id)
                    with zip_ref.open(sub_filename) as sub_file_ref:
                        upload_result = lanying_file_storage.put_object(object_name, sub_file_ref, sub_file_size)
                        if upload_result["result"] == "error":
                            lanying_embedding.update_trace_field(trace_id, "status", "error")
                            lanying_embedding.update_trace_field(trace_id, "message", "upload embedding sub file error")
                            task_error("fail to upload sub file : app_id={app_id}, embedding_name={embedding_name}, embedding_uuid={embedding_uuid},object_name:{object_name}")
                        tasks.append((object_name, sub_filename, doc_id))
    elif ext in [".html", ".htm", ".csv"]:
        file_stat = os.stat(temp_filename)
        file_size = file_stat.st_size
        doc_id = lanying_embedding.generate_doc_id(embedding_uuid)
        object_name = os.path.join(f"{embedding_doc_dir}/{app_id}/{embedding_uuid}/{doc_id}{ext}")
        lanying_embedding.create_doc_info(embedding_uuid, origin_filename, object_name, doc_id, file_size)
        lanying_embedding.add_trace_doc_id(trace_id, doc_id)
        upload_result = lanying_file_storage.upload(object_name, temp_filename)
        if upload_result["result"] == "error":
            lanying_embedding.update_trace_field(trace_id, "status", "error")
            lanying_embedding.update_trace_field(trace_id, "message", "upload embedding file error")
            task_error(f"fail to upload embedding: app_id={app_id}, embedding_name={embedding_name}, embedding_uuid={embedding_uuid}, url={url}, message:{upload_result['message']}")
        tasks.append((object_name, origin_filename, doc_id))
    for now_object_name, now_origin_filename, now_doc_id in tasks:
        lanying_embedding.add_doc_to_embedding(embedding_uuid, now_doc_id)
        process_embedding_file.apply_async(args = [trace_id, app_id, embedding_uuid, now_object_name, now_origin_filename, now_doc_id])

@app.task
def process_embedding_file(trace_id, app_id, embedding_uuid, object_name, origin_filename, doc_id):
    embedding_uuid_info = lanying_embedding.get_embedding_uuid_info(embedding_uuid)
    if "status" not in embedding_uuid_info:
        return
    doc_info = lanying_embedding.get_doc(embedding_uuid, doc_id)
    if "status" not in doc_info:
        return
    lanying_embedding.update_doc_field(embedding_uuid, doc_id, "status", "processing")
    _,ext = os.path.splitext(origin_filename)
    temp_filename = os.path.join(f"{download_dir}/sub-{app_id}-{embedding_uuid}-{uuid.uuid4()}{ext}")
    download_result = lanying_file_storage.download(object_name, temp_filename)
    if download_result["result"] == "error":
        task_error(f"fail to download embedding: trace_id={trace_id}, app_id={app_id}, embedding_name={embedding_uuid}, object_name={object_name}, message:{download_result['message']}")
    lanying_embedding.process_embedding_file(trace_id, app_id, embedding_uuid, temp_filename, origin_filename, doc_id)

def task_error(message):
    logging.error(message)
    raise Exception(message)

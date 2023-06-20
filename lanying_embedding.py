import lanying_redis
import logging
import uuid
import time
import re
import tiktoken
from markdownify import MarkdownConverter
import openai
import os
import random
import numpy as np
from redis.commands.search.query import Query
import pandas as pd
import lanying_config
from pdfminer.high_level import extract_text


global_embedding_rate_limit = int(os.getenv("EMBEDDING_RATE_LIMIT", "30"))
global_openai_base = os.getenv("EMBEDDING_OPENAI_BASE", "https://lanying-connector.lanyingim.com/v1")

tokenizer = tiktoken.get_encoding("cl100k_base")

class IgnoringScriptConverter(MarkdownConverter):
    """
    Create a custom MarkdownConverter that ignores script tags
    """
    def convert_script(self, el, text, convert_as_inline):
        return ''
    def convert_style(self, el, text, convert_as_inline):
        return ''


# Create shorthand method for conversion
def md(html, **options):
    return IgnoringScriptConverter(**options).convert(html)

def create_embedding(app_id, embedding_name, max_block_size = 500, algo="COSINE", admin_user_ids = [], preset_name = ''):
    logging.info("start create embedding: app_id:{app_id}, embedding_name:{embedding_name}, max_block_size:{max_block_size},algo:{algo},admin_user_ids:{admin_user_ids},preset_name:{preset_name}")
    if app_id is None:
        app_id = ""
    old_embedding_name_info = get_embedding_name_info(app_id, embedding_name)
    if old_embedding_name_info:
        return {'result':"error", 'message': 'embedding_name exist'}
    now = int(time.time())
    redis = lanying_redis.get_redis_stack_connection()
    embedding_uuid = generate_embedding_id()
    index_key = get_embedding_index_key(embedding_uuid)
    data_prefix_key = get_embedding_data_prefix_key(embedding_uuid)
    redis.hmset(get_embedding_name_key(app_id, embedding_name), {
        "app_id":app_id,
        "embedding_name": embedding_name,
        "embedding_uuid": embedding_uuid,
        "time": now,
        "status": "ok",
        "admin_user_ids": ",".join([str(admin_user_id) for admin_user_id in admin_user_ids]),
        "preset_name":preset_name,
        "embedding_max_tokens":2048,
        "embedding_max_blocks":5,
        "embedding_content": "请严格按照下面的知识回答我之后的所有问题:"
    })
    redis.rpush(get_embedding_names_key(app_id), embedding_name)
    redis.hmset(get_embedding_uuid_key(embedding_uuid),
                {"app_id": app_id,
                 "embedding_name": embedding_name,
                "index": index_key,
                "prefix": data_prefix_key,
                "max_block_size": max_block_size,
                "algo": algo,
                "size": 0,
                "doc_id_seq": 0,
                "block_id_seq":0,
                "embedding_count":0,
                "embedding_size": 0,
                "text_size": 0,
                "time": now,
                "status": "ok"})
    result = redis.execute_command("FT.CREATE", index_key, "prefix", "1", data_prefix_key, "SCHEMA","text","TEXT", "doc_id", "TAG", "embedding","VECTOR", "HNSW", "6", "TYPE", "FLOAT64","DIM", "1536", "DISTANCE_METRIC",algo)
    update_app_embedding_admin_users(app_id, admin_user_ids)
    bind_preset_name(app_id, preset_name, embedding_name)
    logging.info(f"create_embedding success: app_id:{app_id}, embedding_name:{embedding_name}, embedding_uuid:{embedding_uuid} ft.create.result{result}")
    return {'result':'ok', 'embedding_uuid':embedding_uuid}

def delete_embedding(app_id, embedding_name):
    embedding_name_info = get_embedding_name_info(app_id, embedding_name)
    if embedding_name_info:
        embedding_uuid = embedding_name_info["embedding_uuid"]
        embedding_uuid_info = get_embedding_uuid_info(embedding_uuid)
        if embedding_uuid_info:
            doc_id_list = get_embedding_doc_id_list(embedding_uuid, 0, -1)
            if len(doc_id_list) == 0:
                redis = lanying_redis.get_redis_stack_connection()
                redis.lrem(get_embedding_names_key(app_id), 1, embedding_name)
                redis.delete(get_embedding_uuid_key(embedding_uuid))
                redis.delete(get_embedding_name_key(app_id, embedding_name))
                return True
    return False

def configure_embedding(app_id, embedding_name, admin_user_ids, preset_name, embedding_max_tokens, embedding_max_blocks, embedding_content, new_embedding_name):
    embedding_name_info = get_embedding_name_info(app_id, embedding_name)
    if embedding_name_info is None:
        return {'result':"error", 'message': 'embedding_name not exist'}
    redis = lanying_redis.get_redis_stack_connection()
    if new_embedding_name != embedding_name and new_embedding_name != "":
        new_embedding_name_info = get_embedding_name_info(app_id, new_embedding_name)
        if new_embedding_name_info:
            logging.info(f"configure_embedding | new_embedding_name_info exists:{new_embedding_name_info}")
            return {'result':"error", 'message': 'new_embedding_name exist'}
        else:
            redis.rename(get_embedding_name_key(app_id,embedding_name), get_embedding_name_key(app_id,new_embedding_name))
            unbind_preset_name(app_id, embedding_name)
            list_key = get_embedding_names_key(app_id)
            redis.lrem(list_key, 1, embedding_name)
            redis.rpush(list_key, new_embedding_name)
            embedding_name = new_embedding_name
    redis.hmset(get_embedding_name_key(app_id, embedding_name), {
        "admin_user_ids": ",".join([str(admin_user_id) for admin_user_id in admin_user_ids]),
        "preset_name":preset_name,
        "embedding_name": embedding_name,
        "embedding_max_tokens":embedding_max_tokens,
        "embedding_max_blocks":embedding_max_blocks,
        "embedding_content": embedding_content
    })
    update_app_embedding_admin_users(app_id, admin_user_ids)
    bind_preset_name(app_id, preset_name, embedding_name)
    return {"result":"ok"}

def list_embeddings(app_id):
    redis = lanying_redis.get_redis_stack_connection()
    list_key = get_embedding_names_key(app_id)
    embedding_names = redis_lrange(redis, list_key, 0, -1)
    result = []
    for embedding_name in embedding_names:
        embedding_info = get_embedding_name_info(app_id, embedding_name)
        if embedding_info:
            embedding_info['admin_user_ids'] = embedding_info['admin_user_ids'].split(',')
            embedding_uuid = embedding_info["embedding_uuid"]
            embedding_uuid_info = get_embedding_uuid_info(embedding_uuid)
            for key in ["max_block_size","algo","embedding_count","embedding_size","text_size", "token_cnt", "preset_name", "embedding_max_tokens", "embedding_max_blocks", "embedding_content", "char_cnt", "storage_file_size"]:
                if key in embedding_uuid_info:
                    embedding_info[key] = embedding_uuid_info[key]
            if "embedding_content" not in embedding_info:
                embedding_info["embedding_content"] = "请严格按照下面的知识回答我之后的所有问题:"
            result.append(embedding_info)
    return result

def search_embeddings(app_id, embedding_name, embedding, max_tokens = 2048, max_blocks = 10):
    if max_blocks > 100:
        max_blocks = 100
    result = check_storage_size(app_id)
    if result['result'] == 'error':
        logging.info(f"search_embeddings | skip search for exceed storage limit app, app_id:{app_id}, embedding_name:{embedding_name}")
        return []
    redis = lanying_redis.get_redis_stack_connection()
    if redis:
        embedding_index = get_embedding_index(app_id, embedding_name)
        if embedding_index:
            base_query = f"*=>[KNN {max_blocks} @embedding $vector AS vector_score]"
            query = Query(base_query).sort_by("vector_score").return_fields("text", "vector_score", "filename","parent_id", "num_of_tokens", "summary","doc_id").paging(0,max_blocks).dialect(2)
            results = redis.ft(embedding_index).search(query, query_params={"vector": np.array(embedding).tobytes()})
            # logging.info(f"topk result:{results.docs[:1]}")
            ret = []
            now_tokens = 0
            blocks_num = 0
            for doc in results.docs:
                now_tokens += int(doc.num_of_tokens)
                blocks_num += 1
                logging.info(f"search_embeddings count token: now_tokens:{now_tokens}, num_of_tokens:{int(doc.num_of_tokens)},blocks_num:{blocks_num}")
                if now_tokens > max_tokens:
                    break
                if blocks_num > max_blocks:
                    break
                ret.append(doc)
            return ret
    return []

def get_preset_embedding_infos(app_id, preset_name):
    redis = lanying_redis.get_redis_stack_connection()
    key = get_preset_name_key(app_id)
    bind_infos = redis_hgetall(redis, key)
    embedding_infos = []
    for now_embedding_name, now_preset_name in bind_infos.items():
        if now_preset_name == preset_name:
            embedding_info = get_embedding_name_info(app_id, now_embedding_name)
            if embedding_info:
                embedding_info["embedding_name"] = now_embedding_name
                embedding_info["embedding_max_tokens"] = int(embedding_info.get("embedding_max_tokens","2048"))
                embedding_info["embedding_max_blocks"] = int(embedding_info.get("embedding_max_blocks", "5"))
                embedding_infos.append(embedding_info)
    return embedding_infos

def get_embedding_index(app_id, embedding_name):
    embedding_name_info = get_embedding_name_info(app_id, embedding_name)
    if embedding_name_info:
        embedding_uuid = embedding_name_info["embedding_uuid"]
        embedding_uuid_info = get_embedding_uuid_info(embedding_uuid)
        if embedding_uuid_info:
            return embedding_uuid_info["index"]
    global_embedding_name_info = get_global_embedding_name_info(embedding_name)
    if global_embedding_name_info:
        return global_embedding_name_info["index"]
    return None

def get_global_embedding_name_info(embedding_name):
    redis = lanying_redis.get_redis_stack_connection()
    global_alias_key = get_global_embedding_name_key(embedding_name)
    info = redis_hgetall(redis, global_alias_key)
    if "index" in info:
        return info

def get_global_embedding_name_key(embedding_name):
    return f"embedding_config:alias:{embedding_name}"

def process_embedding_file(trace_id, app_id, embedding_uuid, filename, origin_filename, doc_id):
    redis = lanying_redis.get_redis_stack_connection()
    increase_embedding_doc_field(redis, embedding_uuid, doc_id, "process_count", 1)
    _,ext = os.path.splitext(origin_filename)
    embedding_uuid_info = get_embedding_uuid_info(embedding_uuid)
    # logging.info(f"process_embedding_file | config:{embedding_uuid_info}")
    if embedding_uuid_info:
        try:
            if ext in [".html", ".htm"]:
                process_html(embedding_uuid_info, app_id, embedding_uuid, filename, origin_filename, doc_id)
            elif ext in [".csv"]:
                process_csv(embedding_uuid_info, app_id, embedding_uuid, filename, origin_filename, doc_id)
            elif ext in [".txt"]:
                process_txt(embedding_uuid_info, app_id, embedding_uuid, filename, origin_filename, doc_id)
            elif ext in [".pdf"]:
                process_pdf(embedding_uuid_info, app_id, embedding_uuid, filename, origin_filename, doc_id)
            elif ext in [".md"]:
                process_markdown(embedding_uuid_info, app_id, embedding_uuid, filename, origin_filename, doc_id)
        except Exception as e:
            increase_embedding_doc_field(redis, embedding_uuid, doc_id, "fail_count", 1)
            raise e
        increase_embedding_doc_field(redis, embedding_uuid, doc_id, "succ_count", 1)
        update_doc_field(embedding_uuid, doc_id, "status", "finish")
    else:
        logging.info(f"process_embedding_file embedding_uuid not exist | embedding_uuid:{embedding_uuid}")

def generate_embedding_id():
    redis = lanying_redis.get_redis_stack_connection()
    return redis.incrby("embedding_id_generator", 1)

def get_embedding_name_info(app_id, embedding_name):
    redis = lanying_redis.get_redis_stack_connection()
    key = get_embedding_name_key(app_id, embedding_name)
    info = redis_hgetall(redis, key)
    if "embedding_uuid" in info:
        return info
    return None

def get_embedding_uuid_info(embedding_uuid):
    redis = lanying_redis.get_redis_stack_connection()
    key = get_embedding_uuid_key(embedding_uuid)
    info = redis_hgetall(redis, key)
    if "index" in info:
        return info
    return None

def update_embedding_uuid_info(embedding_uuid, field, value):
    redis = lanying_redis.get_redis_stack_connection()
    key = get_embedding_uuid_key(embedding_uuid)
    redis.hset(key, field, value)

def remove_space_line(text):
    lines = text.split('\n')
    new_lines = [line for line in lines if not re.match(r'^\s*$', line)]
    return '\n'.join(new_lines)

def process_html(config, app_id, embedding_uuid, filename, origin_filename, doc_id):
    with open(filename, "r") as f:
        html = f.read()
        process_markdown_content(config, app_id, embedding_uuid, origin_filename, doc_id, md(html))

def process_markdown_content(config, app_id, embedding_uuid, origin_filename, doc_id, markdown):
    markdown = remove_space_line(markdown)
    rule = config.get('block_split_rule',"^#{1,3} ")
    blocks = []
    total_tokens = 0
    for block in re.split(rule ,markdown, flags=re.MULTILINE):
        block_tokens, block_blocks = process_block(config, block)
        total_tokens += block_tokens
        blocks.extend(block_blocks)
    redis = lanying_redis.get_redis_stack_connection()
    update_progress_total(redis, get_embedding_doc_info_key(embedding_uuid, doc_id), len(blocks))
    insert_embeddings(config, app_id, embedding_uuid, origin_filename, doc_id, blocks, redis)

def process_markdown(config, app_id, embedding_uuid, filename, origin_filename, doc_id):
    with open(filename, 'r', encoding='utf-8') as f:
        content = f.read()
        process_markdown_content(config, app_id, embedding_uuid, origin_filename, doc_id, content)

def process_txt(config, app_id, embedding_uuid, filename, origin_filename, doc_id):
    blocks = []
    total_tokens = 0
    with open(filename, 'r', encoding='utf-8') as f:
        content = f.read()
        total_tokens, blocks = process_block(config, content)
    redis = lanying_redis.get_redis_stack_connection()
    update_progress_total(redis, get_embedding_doc_info_key(embedding_uuid, doc_id), len(blocks))
    insert_embeddings(config, app_id, embedding_uuid, origin_filename, doc_id, blocks, redis)


def process_pdf(config, app_id, embedding_uuid, filename, origin_filename, doc_id):
    blocks = []
    total_tokens = 0
    content = extract_text(filename)
    total_tokens, blocks = process_block(config, content)
    redis = lanying_redis.get_redis_stack_connection()
    update_progress_total(redis, get_embedding_doc_info_key(embedding_uuid, doc_id), len(blocks))
    insert_embeddings(config, app_id, embedding_uuid, origin_filename, doc_id, blocks, redis)

def process_csv(config, app_id, embedding_uuid, filename, origin_filename, doc_id):
    logging.info(f"start process_csv: embedding_uuid={embedding_uuid}, filename={filename}")
    df = pd.read_csv(filename)
    size = len(df)
    logging.info(f"embeddings: size={size}")
    total_blocks = 0
    total_tokens = 0
    redis = lanying_redis.get_redis_stack_connection()
    for i, row in df.iterrows():
        if 'text' in row:
            block_tokens, block_blocks = process_block(config, row['text'])
            total_tokens += block_tokens
            total_blocks += len(block_blocks)
            update_progress_total(redis, get_embedding_doc_info_key(embedding_uuid, doc_id), total_blocks)
            insert_embeddings(config, app_id, embedding_uuid, origin_filename, doc_id, block_blocks, redis)

def insert_embeddings(config, app_id, embedding_uuid, origin_filename, doc_id, blocks, redis):
    openai_secret_key = config["openai_secret_key"]
    is_dry_run = config.get("dry_run", "false") == "true"
    logging.info(f"insert_embeddings | app_id:{app_id}, embedding_uuid:{embedding_uuid}, origin_filename:{origin_filename}, doc_id:{doc_id}, is_dry_run:{is_dry_run}, block_count:{len(blocks)}, dry_run_from_config:{config.get('dry_run', 'None')}")
    for token_cnt,text in blocks:
        doc_info = get_doc(embedding_uuid, doc_id)
        if doc_info:
            block_id = generate_block_id(embedding_uuid)
            maybe_rate_limit(5)
            embedding = fetch_embedding(openai_secret_key, text, is_dry_run)
            key = get_embedding_data_key(embedding_uuid, block_id)
            embedding_bytes = np.array(embedding).tobytes()
            redis.hmset(key, {"text":text,
                            "embedding":embedding_bytes,
                            "doc_id": doc_id,
                            "num_of_tokens": token_cnt,
                            "filename": origin_filename,
                            "summary": "{}"})
            embedding_size = len(embedding_bytes)
            text_size = text_byte_size(text)
            char_cnt = len(text)
            increase_embedding_uuid_field(redis, embedding_uuid, "embedding_count", 1)
            increase_embedding_uuid_field(redis, embedding_uuid, "embedding_size", embedding_size)
            increase_embedding_uuid_field(redis, embedding_uuid, "text_size", text_size)
            increase_embedding_uuid_field(redis, embedding_uuid, "char_cnt", char_cnt)
            increase_embedding_uuid_field(redis, embedding_uuid, "token_cnt", token_cnt)
            increase_embedding_doc_field(redis, embedding_uuid, doc_id, "embedding_count", 1)
            increase_embedding_doc_field(redis, embedding_uuid, doc_id, "embedding_size", embedding_size)
            increase_embedding_doc_field(redis, embedding_uuid, doc_id, "text_size", text_size)
            increase_embedding_doc_field(redis, embedding_uuid, doc_id, "char_cnt", char_cnt)
            increase_embedding_doc_field(redis, embedding_uuid, doc_id, "token_cnt", token_cnt)
            update_progress(redis, get_embedding_doc_info_key(embedding_uuid, doc_id), 1)

def fetch_embedding(openai_secret_key, text, is_dry_run=False, retry = 10, sleep = 0.2, sleep_multi=1.7):
    if is_dry_run:
        return [random.uniform(0, 1) for i in range(1536)]
    openai.api_key = openai_secret_key
    openai.api_base = global_openai_base
    try:
        return openai.Embedding.create(input=text, engine='text-embedding-ada-002')['data'][0]['embedding']
    except Exception as e:
        if retry > 0:
            time.sleep(sleep)
            return fetch_embedding(openai_secret_key, text, is_dry_run, retry-1, sleep * sleep_multi, sleep_multi)
        raise e

def process_block(config, block):
    lines = []
    token_cnt = 0
    blocks = []
    total_tokens = 0
    max_block_size = get_max_token_count(config)
    for line in block.split('\n'):
        line_token_count = num_of_tokens(line + "\n")
        if token_cnt + line_token_count > max_block_size:
            if token_cnt > 0:
                now_block = "\n".join(lines) + "\n"
                blocks.append((token_cnt, now_block))
                total_tokens += token_cnt
                lines = []
                token_cnt = 0
        if line_token_count > max_block_size:
            logging.info(f"processing too long line: {line}")
            blocks.extend(process_line(line, max_block_size))
            continue
        lines.append(line)
        token_cnt += line_token_count
    if token_cnt > 0:
        now_block = "\n".join(lines) + "\n"
        blocks.append((token_cnt, now_block))
        total_tokens += token_cnt
    return (total_tokens, blocks)

def process_line(text, max_block_size):
    result = []
    current_str = ""
    token_cnt = 0
    char_cnt = 0
    for char in text:
        if token_cnt >= max_block_size:
            result.append((token_cnt, current_str))
            current_str = ""
            token_cnt = 0
            char_cnt = 0
        current_str += char
        char_cnt += 1
        if token_cnt >= max_block_size - 20 or char_cnt % 20 == 0:
            token_cnt = num_of_tokens(current_str)
    if len(current_str) > 0:
        token_cnt = num_of_tokens(current_str)
        result.append((token_cnt, current_str))
    return result

def get_max_token_count(config):
    max_block_size = int(config.get('max_block_size', "350"))
    max_token_count = word_num_to_token_num(max_block_size)
    return max_token_count

def word_num_to_token_num(word_num):
    return round(word_num * 1.3)

def generate_block_id(embedding_uuid):
    key = get_embedding_uuid_key(embedding_uuid)
    redis = lanying_redis.get_redis_stack_connection()
    return redis.hincrby(key, "block_id_seq", 1)

def num_of_tokens(str):
    return len(tokenizer.encode(str))

def maybe_rate_limit(retry):
    redis = lanying_redis.get_redis_stack_connection()
    now = time.time()
    key = f"embedding:rate_limit:{int(now)}"
    count = redis.incrby(key, 1)
    if count > global_embedding_rate_limit and retry > 0:
        time.sleep(int(now)+1 - now + 0.1 * random.random())
        maybe_rate_limit(retry-1)

def create_trace_id():
    redis = lanying_redis.get_redis_stack_connection()
    for i in range(100):
        trace_id = str(uuid.uuid4())
        key = trace_id_key(trace_id)
        if redis.hsetnx(key, "status", "wait") > 0:
            return trace_id

def update_trace_field(trace_id, field, value):
    redis = lanying_redis.get_redis_stack_connection()
    key = trace_id_key(trace_id)
    redis.hset(key, field, value)

def get_all_trace_fields(trace_id):
    redis = lanying_redis.get_redis_stack_connection()
    key = trace_id_key(trace_id)
    return redis_hgetall(redis, key)

def add_trace_doc_id(trace_id, doc_id):
    redis = lanying_redis.get_redis_stack_connection()
    key = trace_doc_key(trace_id)
    redis.rpush(key, doc_id)

def clear_trace_doc_id(trace_id):
    redis = lanying_redis.get_redis_stack_connection()
    key = trace_doc_key(trace_id)
    redis.delete(key)

def get_trace_doc_ids(trace_id):
    redis = lanying_redis.get_redis_stack_connection()
    key = trace_doc_key(trace_id)
    return redis_lrange(redis, key, 0, -1)

def trace_id_key(trace_id):
    return f"lanying_trace_id:{trace_id}"

def trace_doc_key(trace_id):
    return f"lanying_trace_doc:{trace_id}"

def update_progress(redis, key, value):
    redis.hincrby(key, "progress_finish", value)

def update_progress_total(redis, key, total):
    redis.hset(key, "progress_total", total)

def increase_embedding_uuid_field(redis, embedding_uuid, field, value):
    redis.hincrby(get_embedding_uuid_key(embedding_uuid), field, value)

def update_app_embedding_admin_users(app_id, admin_user_ids):
    redis = lanying_redis.get_redis_stack_connection()
    key = get_app_embedding_admin_user_key(app_id)
    for user_id in admin_user_ids:
        redis.hincrby(key, user_id, 1)

def bind_preset_name(app_id, preset_name, embedding_name):
    redis = lanying_redis.get_redis_stack_connection()
    key = get_preset_name_key(app_id)
    redis.hset(key, embedding_name, preset_name)

def unbind_preset_name(app_id, embedding_name):
    redis = lanying_redis.get_redis_stack_connection()
    key = get_preset_name_key(app_id)
    redis.hdel(key, embedding_name)

def is_app_embedding_admin_user(app_id, user_id):
    redis = lanying_redis.get_redis_stack_connection()
    key = get_app_embedding_admin_user_key(app_id)
    return redis.hget(key, user_id) is not None

def get_embedding_names_key(app_id):
    return f"embedding_names:{app_id}"

def get_preset_name_key(app_id):
    return f"preset_names:{app_id}"

def get_embedding_name_key(app_id, embedding_name):
    return f"embedding_name:{app_id}:{embedding_name}"

def get_embedding_uuid_key(embedding_uuid):
    return f"embedding_uuid:{embedding_uuid}"

def get_embedding_index_key(embedding_uuid):
    return f"embedding_index:{embedding_uuid}"

def get_embedding_data_prefix_key(embedding_uuid):
    return f"embedding_data:{embedding_uuid}:"

def get_embedding_data_key(embedding_uuid, block_id):
    return f"embedding_data:{embedding_uuid}:{block_id}"

def get_app_embedding_admin_user_key(app_id):
    return f"embedding_admin_user_id:{app_id}"

def create_doc_info(embedding_uuid, filename, object_name, doc_id, file_size):
    redis = lanying_redis.get_redis_stack_connection()
    info_key = get_embedding_doc_info_key(embedding_uuid, doc_id)
    redis.hmset(info_key, {"filename":filename,
                           "object_name":object_name,
                           "time": int(time.time()),
                           "file_size": file_size,
                           "status": "wait"})
def update_doc_field(embedding_uuid, doc_id, field, value):
    redis = lanying_redis.get_redis_stack_connection()
    info_key = get_embedding_doc_info_key(embedding_uuid, doc_id)
    redis.hset(info_key, field, value)

def add_doc_to_embedding(embedding_uuid, doc_id):
    redis = lanying_redis.get_redis_stack_connection()
    list_key = get_embedding_doc_list_key(embedding_uuid)
    redis.rpush(list_key, doc_id)

def delete_doc_from_embedding(app_id, embedding_name, doc_id, task):
    logging.info(f"delete_doc_from_embedding started | app_id:{app_id}, embedding_name:{embedding_name}, doc_id:{doc_id}")
    redis = lanying_redis.get_redis_stack_connection()
    embedding_index = get_embedding_index(app_id, embedding_name)
    if embedding_index:
        embedding_name_info = get_embedding_name_info(app_id, embedding_name)
        embedding_uuid = embedding_name_info["embedding_uuid"]
        doc_info = get_doc(embedding_uuid, doc_id)
        if doc_info:
            task.apply_async(args = [app_id, embedding_name, doc_id, embedding_index, 0])
            list_key = get_embedding_doc_list_key(embedding_uuid)
            redis.lrem(list_key, 2, doc_id)
            restore_storage_size(app_id, embedding_uuid, doc_id)
            info_key = get_embedding_doc_info_key(embedding_uuid, doc_id)
            redis.delete(info_key)
            increase_embedding_uuid_field(redis, embedding_uuid, "embedding_count", -int(doc_info.get("embedding_count", "0")))
            increase_embedding_uuid_field(redis, embedding_uuid, "embedding_size", -int(doc_info.get("embedding_size", "0")))
            increase_embedding_uuid_field(redis, embedding_uuid, "text_size", -int(doc_info.get("text_size", "0")))
            increase_embedding_uuid_field(redis, embedding_uuid, "token_cnt", -int(doc_info.get("token_cnt", "0")))
            increase_embedding_uuid_field(redis, embedding_uuid, "char_cnt", -int(doc_info.get("char_cnt", "0")))
            return True
    return False

def search_doc_data_and_delete(app_id, embedding_name, doc_id, embedding_index, last_total):
    redis = lanying_redis.get_redis_stack_connection()
    base_query = query_by_doc_id(doc_id)
    query = Query(base_query).no_content().paging(0, 200).dialect(2)
    results = redis.ft(embedding_index).search(query)
    logging.info(f"search for delete  | app_id:{app_id}, embedding_name:{embedding_name}, doc_id:{doc_id}, last_total:{last_total}, result:{results}")
    if results.total == 0 or results.total == last_total:
        logging.info(f"search for delete stop  | app_id:{app_id}, embedding_name:{embedding_name}, doc_id:{doc_id}, last_total:{last_total}")
        return
    keys = []
    for doc in results.docs:
        keys.append(doc.id)
    if len(keys) > 0:
        redis.delete(*keys)
    if len(keys) < results.total:
        search_doc_data_and_delete(app_id, embedding_name, doc_id, embedding_index, results.total)
    else:
        logging.info(f"search for delete stop for last page | app_id:{app_id}, embedding_name:{embedding_name}, doc_id:{doc_id}, last_total:{last_total}")

def query_by_doc_id(doc_id):
    if len(doc_id) < 30:
        new_doc_id = doc_id.replace('-','\\-')
        return "@doc_id:{"+new_doc_id+"}"
    else: # for deprecated doc_id format
        return "@doc_id:{"+doc_id+"}"

def get_doc(embedding_uuid, doc_id):
    redis = lanying_redis.get_redis_stack_connection()
    info_key = get_embedding_doc_info_key(embedding_uuid, doc_id)
    info = redis_hgetall(redis, info_key)
    if "filename" in info:
        return info
    return None

def check_storage_size(app_id):
    redis = lanying_redis.get_redis_stack_connection()
    storage_limit = get_app_config_int(app_id, "lanying_connector.storage_limit")
    storage_payg = get_app_config_int(app_id, "lanying_connector.storage_payg")
    if storage_payg == 1 and storage_limit > 0:
        return {'result':'ok'}
    now_storage_size = increase_app_storage_file_size(app_id, 0)
    if storage_limit == 0 or now_storage_size > storage_limit * 1024 * 1024 * 1.1:
        return {'result': 'error'}
    return {'result':'ok'}

def add_storage_size(app_id, embedding_uuid, doc_id, file_size):
    redis = lanying_redis.get_redis_stack_connection()
    storage_limit = get_app_config_int(app_id, "lanying_connector.storage_limit")
    storage_payg = get_app_config_int(app_id, "lanying_connector.storage_payg")
    if storage_payg == 1 and storage_limit > 0:
        increase_app_storage_file_size(app_id, file_size)
        increase_embedding_doc_field(redis, embedding_uuid, doc_id, "storage_file_size", file_size)
        increase_embedding_uuid_field(redis, embedding_uuid, "storage_file_size", file_size)
        return {'result':'ok'}
    now_storage_size = increase_app_storage_file_size(app_id, 0)
    if now_storage_size + file_size > storage_limit * 1024 * 1024:
        return {'result': 'error'}
    now_storage_size = increase_app_storage_file_size(app_id, file_size)
    if now_storage_size > storage_limit * 1024 * 1024:
        increase_app_storage_file_size(app_id, -file_size)
        return {'result': 'error'}
    increase_embedding_doc_field(redis, embedding_uuid, doc_id, "storage_file_size", file_size)
    increase_embedding_uuid_field(redis, embedding_uuid, "storage_file_size", file_size)
    return {'result':'ok'}

def restore_storage_size(app_id, embedding_uuid, doc_id):
    redis = lanying_redis.get_redis_stack_connection()
    doc_info = get_doc(embedding_uuid, doc_id)
    if doc_info:
        storage_file_size = int(doc_info.get('storage_file_size', "0"))
        if storage_file_size > 0:
            result = increase_embedding_doc_field(redis, embedding_uuid, doc_id, "storage_file_size", -storage_file_size)
            if result >= 0:
                increase_embedding_uuid_field(redis, embedding_uuid, "storage_file_size", -storage_file_size)
                increase_app_storage_file_size(app_id, -storage_file_size)

def increase_app_storage_file_size(app_id, value):
    redis = lanying_redis.get_redis_stack_connection()
    key = get_app_embedding_app_info_key(app_id)
    result = redis.hincrby(key, "storage_file_size", value)
    storage_file_size_max = redis.hincrby(key, "storage_file_size_max", 0)
    if storage_file_size_max < result:
        redis.hset(key, "storage_file_size_max", result)
    return result

def increase_embedding_doc_field(redis, embedding_uuid, doc_id, field, value):
    return redis.hincrby(get_embedding_doc_info_key(embedding_uuid, doc_id), field, value)

def generate_doc_id(embedding_uuid):
    key = get_embedding_uuid_key(embedding_uuid)
    redis = lanying_redis.get_redis_stack_connection()
    doc_id_seq = redis.hincrby(key, "doc_id_seq", 1)
    return f"{embedding_uuid}-{doc_id_seq}"

def get_embedding_doc_info_list(app_id, embedding_name, start, end):
    embedding_name_info = get_embedding_name_info(app_id, embedding_name)
    total = 0
    result = []
    if embedding_name_info:
        embedding_uuid = embedding_name_info["embedding_uuid"]
        list_key = get_embedding_doc_list_key(embedding_uuid)
        redis = lanying_redis.get_redis_stack_connection()
        total = redis.llen(list_key)
        doc_id_list = redis_lrange(redis, list_key, start, end)
        for doc_id in doc_id_list:
            doc = get_doc(embedding_uuid,doc_id)
            if doc:
                doc['doc_id'] = doc_id
                logging.info(f"doc_info: app_id:{app_id}, embedding_name:{embedding_name}, embedding_uuid:{embedding_uuid}, doc_id:{doc_id}, info:{doc}")
                result.append(doc)
    return (total, result)

def get_embedding_doc_id_list(embedding_uuid, start, end):
    list_key = get_embedding_doc_list_key(embedding_uuid)
    redis = lanying_redis.get_redis_stack_connection()
    doc_id_list = redis_lrange(redis, list_key, start, end)
    return doc_id_list

def get_embedding_doc_list_key(embedding_uuid):
    return f"embedding_doc_list:{embedding_uuid}"

def get_embedding_doc_info_key(embedding_uuid, doc_id):
    return f"embedding_doc_info:{embedding_uuid}:{doc_id}"

def get_app_embedding_app_info_key(app_id):
    return f"embedding_app_info:{app_id}"

def redis_lrange(redis, key, start, end):
    return [bytes.decode('utf-8') for bytes in redis.lrange(key, start, end)]

def redis_hgetall(redis, key):
    kvs = redis.hgetall(key)
    ret = {}
    if kvs:
        for k,v in kvs.items():
            ret[k.decode('utf-8')] = v.decode('utf-8')
    return ret

def text_byte_size(text):
    return len(text.encode('utf-8'))

def get_embedding_usage(app_id):
    redis = lanying_redis.get_redis_stack_connection()
    key = get_app_embedding_app_info_key(app_id)
    return redis_hgetall(redis, key)

def set_embedding_usage(app_id, storage_file_size_max):
    redis = lanying_redis.get_redis_stack_connection()
    key = get_app_embedding_app_info_key(app_id)
    redis.hset(key, "storage_file_size_max", storage_file_size_max)
    return True

def save_app_config(app_id, key, value):
    if key.startswith("lanying_connector."):
        redis = lanying_redis.get_redis_stack_connection()
        name = get_app_config_key(app_id)
        redis.hset(name, key, value)

def get_app_config_int(app_id, key):
    redis = lanying_redis.get_redis_stack_connection()
    name = get_app_config_key(app_id)
    return redis.hincrby(name, key, 0)

def get_app_config_key(app_id):
    return f"embedding:app_config:{app_id}"

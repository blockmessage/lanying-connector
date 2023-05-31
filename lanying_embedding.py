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

def create_embedding(app_id, embedding_name, max_block_size = 500, algo="COSINE", admin_user_ids = []):
    if app_id is None:
        app_id = ""
    if is_embedding_name_exist(app_id, embedding_name):
        return {'result':"error", 'message': 'embedding_name exist'}
    now = int(time.time())
    redis = lanying_redis.get_redis_stack_connection()
    embedding_uuid = generate_embedding_uuid()
    index_key = get_embedding_index_key(embedding_uuid)
    data_prefix_key = get_embedding_data_prefix_key(embedding_uuid)
    redis.hmset(get_embedding_name_key(app_id, embedding_name), {
        "app_id":app_id,
        "embedding_name": embedding_name,
        "embedding_uuid": embedding_uuid,
        "time": now,
        "status": "ok",
        "admin_user_ids": ",".join([str(admin_user_id) for admin_user_id in admin_user_ids])
    })
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
                "doc_count": 0,
                "embedding_count":0,
                "embedding_size": 0,
                "text_size": 0,
                "time": now,
                "status": "ok"})
    result = redis.execute_command("FT.CREATE", index_key, "prefix", "1", data_prefix_key, "SCHEMA","text","TEXT", "doc_id", "TAG", "embedding","VECTOR", "HNSW", "6", "TYPE", "FLOAT64","DIM", "1536", "DISTANCE_METRIC",algo)
    update_app_embedding_admin_users(app_id, admin_user_ids)
    logging.debug(f"create_embedding success: app_id:{app_id}, embedding_name:{embedding_name}, embedding_uuid:{embedding_uuid} ft.create.result{result}")
    return {'result':'ok', 'embedding_uuid':embedding_uuid}

def delete_embedding(app_id, embedding_name):
    pass

def search_embeddings(app_id, embedding_name, embedding, max_tokens = 2048, max_blocks = 10):
    redis = lanying_redis.get_redis_stack_connection()
    if redis:
        embedding_index = get_embedding_index(app_id, embedding_name)
        if embedding_index:
            base_query = f"*=>[KNN {max_blocks} @embedding $vector AS vector_score]"
            query = Query(base_query).sort_by("vector_score").return_fields("text", "vector_score", "filename","parent_id", "num_of_tokens", "summary").paging(0,max_blocks).dialect(2)
            results = redis.ft(embedding_index).search(query, query_params={"vector": np.array(embedding).tobytes()})
            print(f"topk result:{results}")
            ret = []
            now_tokens = 0
            blocks_num = 0
            for doc in results.docs:
                now_tokens += int(doc.num_of_tokens)
                blocks_num += 1
                logging.debug(f"search_embeddings count token: now_tokens:{now_tokens}, num_of_tokens:{int(doc.num_of_tokens)},blocks_num:{blocks_num}")
                if now_tokens > max_tokens:
                    break
                if blocks_num > max_blocks:
                    break
                ret.append(doc)
            return ret
    return []

def get_embedding_index(app_id, embedding_name):
    embedding_name_info = get_embedding_info(app_id, embedding_name)
    if "embedding_uuid" in embedding_name_info:
        embedding_uuid = embedding_name_info["embedding_uuid"]
        embedding_uuid_info = get_embedding_uuid_info(embedding_uuid)
        if "index" in embedding_uuid_info:
            return embedding_uuid_info["index"]
    return None

def add_embedding_file(app_id, embedding_uuid, filename):
    pass

def process_embedding_file(trace_id, app_id, embedding_uuid, filename, origin_filename, doc_id):
    redis = lanying_redis.get_redis_stack_connection()
    increase_embedding_doc_field(redis, embedding_uuid, doc_id, "process_count", 1)
    _,ext = os.path.splitext(origin_filename)
    embedding_uuid_info = get_embedding_uuid_info(embedding_uuid)
    # logging.debug(f"process_embedding_file | config:{embedding_uuid_info}")
    try:
        if ext in [".html", ".htm"]:
            process_embedding_file_html(embedding_uuid_info, app_id, embedding_uuid, filename, origin_filename, doc_id)
    except Exception as e:
        increase_embedding_doc_field(redis, embedding_uuid, doc_id, "fail_count", 1)
        update_doc_field(embedding_uuid, doc_id, "status", "error")
        raise e
    increase_embedding_doc_field(redis, embedding_uuid, doc_id, "succ_count", 1)
    update_doc_field(embedding_uuid, doc_id, "status", "finish")

def delete_doc(doc_id):
    pass

def generate_embedding_uuid():
    redis = lanying_redis.get_redis_stack_connection()
    for i in range(100):
        embedding_uuid = str(uuid.uuid4())
        key = get_embedding_uuid_key(embedding_uuid)
        if redis.hsetnx(key, "status", "reserved") > 0:
            return embedding_uuid

def is_embedding_uuid_exist(embedding_uuid):
    redis = lanying_redis.get_redis_stack_connection()
    return redis.exists(get_embedding_uuid_key(embedding_uuid)) > 0

def get_embedding_info(app_id, embedding_name):
    redis = lanying_redis.get_redis_stack_connection()
    key = get_embedding_name_key(app_id, embedding_name)
    return redis_hgetall(redis, key)

def get_embedding_uuid_info(embedding_uuid):
    redis = lanying_redis.get_redis_stack_connection()
    key = get_embedding_uuid_key(embedding_uuid)
    return redis_hgetall(redis, key)

def update_embedding_uuid_info(embedding_uuid, field, value):
    redis = lanying_redis.get_redis_stack_connection()
    key = get_embedding_uuid_key(embedding_uuid)
    redis.hset(key, field, value)

def is_embedding_name_exist(app_id, embedding_name):
    redis = lanying_redis.get_redis_stack_connection()
    key = get_embedding_name_key(app_id, embedding_name)
    return redis.exists(key) > 0

def remove_space_line(text):
    lines = text.split('\n')
    new_lines = [line for line in lines if not re.match(r'^\s*$', line)]
    return '\n'.join(new_lines)

def process_embedding_file_html(config, app_id, embedding_uuid, filename, origin_filename, doc_id):
    with open(filename, "r") as f:
        html = f.read()
        process_markdown(config, app_id, embedding_uuid, origin_filename, doc_id, md(html))

def process_markdown(config, app_id, embedding_uuid, origin_filename, doc_id, markdown):
    markdown = remove_space_line(markdown)
    rule = config.get('block_split_rule',"^#{1,3} ")
    blocks = []
    total_tokens = 0
    for block in re.split(rule ,markdown, flags=re.MULTILINE):
        block_tokens, block_blocks = process_block(config, embedding_uuid, block)
        total_tokens += block_tokens
        blocks.extend(block_blocks)
    redis = lanying_redis.get_redis_stack_connection()
    update_progress_total(redis, get_embedding_doc_info_key(embedding_uuid, doc_id), len(blocks))
    insert_embeddings(config, app_id, embedding_uuid, origin_filename, doc_id, blocks, total_tokens, redis)

def insert_embeddings(config, app_id, embedding_uuid, origin_filename, doc_id, blocks, total_tokens, redis):
    openai_secret_key = config["openai_secret_key"]
    is_dry_run = config.get("dry_run", "false") == "true"
    logging.debug(f"insert_embeddings | app_id:{app_id}, embedding_uuid:{embedding_uuid}, origin_filename:{origin_filename}, doc_id:{doc_id}, is_dry_run:{is_dry_run}, block_count:{len(blocks)}, dry_run_from_config:{config.get('dry_run', 'None')}")
    for token_cnt,text in blocks:
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
        text_size = len(text)
        increase_embedding_uuid_field(redis, embedding_uuid, "embedding_count", 1)
        increase_embedding_uuid_field(redis, embedding_uuid, "embedding_size", embedding_size)
        increase_embedding_uuid_field(redis, embedding_uuid, "text_size", text_size)
        increase_embedding_doc_field(redis, embedding_uuid, doc_id, "embedding_count", 1)
        increase_embedding_doc_field(redis, embedding_uuid, doc_id, "embedding_size", embedding_size)
        increase_embedding_doc_field(redis, embedding_uuid, doc_id, "text_size", text_size)
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

def process_block(config, embedding_uuid, block):
    lines = []
    token_cnt = 0
    blocks = []
    total_tokens = 0
    max_block_size = int(config.get('max_block_size', "500"))
    for line in block.split('\n'):
        line_token_count = num_of_tokens(line + "\n")
        if token_cnt + line_token_count > max_block_size:
            if token_cnt > 0:
                now_block = "".join(lines)
                blocks.append((token_cnt, now_block))
                total_tokens += token_cnt
                lines = []
                token_cnt = 0
        if line_token_count > max_block_size:
            logging.debug(f"drop too long line: {line}")
            continue
        lines.append(line)
        token_cnt += line_token_count
    if token_cnt > 0:
        now_block = "".join(lines)
        blocks.append((token_cnt, now_block))
        total_tokens += token_cnt
    return (total_tokens, blocks)

def save_block():
    pass

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

def is_app_embedding_admin_user(app_id, user_id):
    redis = lanying_redis.get_redis_stack_connection()
    key = get_app_embedding_admin_user_key(app_id)
    return redis.hget(key, user_id) is not None

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

def get_doc(embedding_uuid, doc_id):
    redis = lanying_redis.get_redis_stack_connection()
    info_key = get_embedding_doc_info_key(embedding_uuid, doc_id)
    return redis_hgetall(redis, info_key)

def increase_embedding_doc_field(redis, embedding_uuid, doc_id, field, value):
    redis.hincrby(get_embedding_doc_info_key(embedding_uuid, doc_id), field, value)

def generate_doc_id(embedding_uuid):
    key = get_embedding_uuid_key(embedding_uuid)
    redis = lanying_redis.get_redis_stack_connection()
    return redis.hincrby(key, "doc_id_seq", 1)

def get_embedding_doc_info_list(app_id, embedding_name, max_count):
    embedding_name_info = get_embedding_info(app_id, embedding_name)
    result = []
    if "embedding_uuid" in embedding_name_info:
        embedding_uuid = embedding_name_info["embedding_uuid"]
        list_key = get_embedding_doc_list_key(embedding_uuid)
        redis = lanying_redis.get_redis_stack_connection()
        doc_id_list = redis_lrange(redis, list_key, -max_count, -1)
        for doc_id in doc_id_list:
            doc = get_doc(embedding_uuid,doc_id)
            doc['doc_id'] = doc_id
            logging.debug(f"doc_info: app_id:{app_id}, embedding_name:{embedding_name}, embedding_uuid:{embedding_uuid}, doc_id:{doc_id}, info:{doc}")
            result.append(doc)
    return result

def get_embedding_doc_list_key(embedding_uuid):
    return f"embedding_doc_list:{embedding_uuid}"

def get_embedding_doc_info_key(embedding_uuid, doc_id):
    return f"embedding_doc_info:{embedding_uuid}:{doc_id}"

def redis_lrange(redis, key, start, end):
    return [bytes.decode('utf-8') for bytes in redis.lrange(key, start, end)]

def redis_hgetall(redis, key):
    kvs = redis.hgetall(key)
    ret = {}
    if kvs:
        for k,v in kvs.items():
            ret[k.decode('utf-8')] = v.decode('utf-8')
    return ret

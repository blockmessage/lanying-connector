from typing import Iterator
from urllib.parse import urlparse
import requests
import logging
from langchain.docstore.document import Document
from bs4 import BeautifulSoup
import uuid
import lanying_redis
from urllib.parse import urljoin

# this file is edit from source of langchain.document_loaders.recursive_url_loader

def create_task(url, advised_task_id = None):
    redis = lanying_redis.get_redis_stack_connection()
    if advised_task_id:
        task_id = advised_task_id
    else:
        task_id = str(uuid.uuid4())
    redis.rpush(to_visit_key(task_id), url)
    redis.sadd(visited_key(task_id), url)
    return task_id

def clean_task(task_id):
    redis = lanying_redis.get_redis_stack_connection()
    redis.delete(to_visit_key(task_id), visited_key(task_id), found_key(task_id))

def info_task(task_id):
    redis = lanying_redis.get_redis_stack_connection()
    return {
        'to_visit': redis.llen(to_visit_key(task_id)),
        'visited': redis.scard(visited_key(task_id)),
        'found': redis.scard(found_key(task_id))
    }

def do_task(task_id, root_url)-> Iterator[Document]:
    redis = lanying_redis.get_redis_stack_connection()
    parse_root_url = urlparse(root_url.strip(' '))
    filter_url = f"{parse_root_url.scheme}://{parse_root_url.netloc}{parse_root_url.path if parse_root_url.path != '' else '/'}"
    visit_filter_url_bytes = redis.get(visit_filter_url_key(task_id))
    is_use_new_visit_filter_url = False
    if visit_filter_url_bytes:
        is_use_new_visit_filter_url = True
        visit_filter_url = visit_filter_url_bytes.decode('utf-8')
    else:
        visit_filter_url = filter_url
    while(True):
        url = redis.lpop(to_visit_key(task_id))
        if url is None:
            if not is_use_new_visit_filter_url:
                last_found_num = redis.scard(found_key(task_id))
                if last_found_num < 4:
                    visit_filter_url = f"{parse_root_url.scheme}://{parse_root_url.netloc}/"
                    if visit_filter_url != filter_url:
                        is_use_new_visit_filter_url = True
                        redis.set(visit_filter_url_key(task_id), visit_filter_url)
                        redis.delete(visited_key(task_id))
                        logging.info(f"try find more | task_id:{task_id}, root_url:{root_url}, filter_url:{filter_url}, visit_filter_url:{visit_filter_url}, last_found_num:{last_found_num}")
                        redis.rpush(to_visit_key(task_id), visit_filter_url)
                        redis.sadd(visited_key(task_id), visit_filter_url)
                        continue
            break
        url = url.decode("utf-8")
        if redis.sismember(found_key(task_id), url):
            continue
        logging.info(f"load_url:  task_id:{task_id}, visit url:{url}, root_url:{root_url},filter_url:{filter_url}, visit_filter_url:{visit_filter_url}")
        try:
            response = requests.get(url,timeout=(20.0, 60.0))
            content_type = response.headers.get('Content-Type')
            if response.status_code == 200 and content_type.startswith('text/html'):
                response_url = response.url
                soup = BeautifulSoup(response.text, "html.parser")
                all_links = [link.get("href") for link in soup.find_all("a")]

                absolute_paths = list(
                    {
                        format_link(urljoin(response_url, link))
                        for link in all_links
                    }
                )

                for link in absolute_paths:
                    # logging.info(f"sub link:{link}, visit_filter_url:{visit_filter_url},filter_url:{filter_url}")
                    if not redis.sismember(visited_key(task_id), link):
                        if link.startswith(visit_filter_url):
                            logging.info(f"add to_visit:  task_id:{task_id}, visit url:{url}, link:{link}, root_url:{root_url},filter_url:{filter_url}, visit_filter_url:{visit_filter_url}")
                            redis.rpush(to_visit_key(task_id), link)
                            redis.sadd(visited_key(task_id), link)
                if url.startswith(filter_url):
                    redis.sadd(found_key(task_id), url)
                    yield Document(page_content=response.text, metadata={'source':url, 'page_bytes':response.content})
                else:
                    yield None
            else:
                logging.info(f"skip for status_code:{response.status_code}, url:{url}, content_type:{content_type}")
        except Exception as e:
            logging.exception(e)

def format_link(url):
    url = url.strip(" ")
    index = url.find("#")
    if index != -1:
        return url[:index]
    return url

def to_visit_key(task_id):
    return f"lanying-embedding:url-loader:task:{task_id}:to_visit"

def visited_key(task_id):
    return f"lanying-embedding:url-loader:task:{task_id}:visited"

def found_key(task_id):
    return f"lanying-embedding:url-loader:task:{task_id}:found"

def visit_filter_url_key(task_id):
    return f"lanying-embedding:url-loader:task:{task_id}:visit_filter_url"
from typing import Iterator
from urllib.parse import urlparse
import requests
import logging
from langchain.docstore.document import Document
from bs4 import BeautifulSoup
import uuid
import lanying_redis
from urllib.parse import urljoin
import json

# this file is edit from source of langchain.document_loaders.recursive_url_loader

def create_task(urls):
    redis = lanying_redis.get_redis_stack_connection()
    task_id = str(uuid.uuid4())
    for url in urls:
        if url.startswith("http://") or url.startswith("https://"):
            redis.rpush(to_visit_key(task_id), to_json({'url':url, 'depth':0}))
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

def do_task(task_id, urls, max_depth, filters)-> Iterator[Document]:
    if len(urls) == 0:
        return
    first_url = urls[0]
    redis = lanying_redis.get_redis_stack_connection()
    parse_first_url= urlparse(first_url.strip(' '))
    root_url = f"{parse_first_url.scheme}://{parse_first_url.netloc}/"
    http_root_url = f"http://{parse_first_url.netloc}/"
    while(True):
        to_visit_bytes = redis.lpop(to_visit_key(task_id))
        if to_visit_bytes is None:
            break
        to_visit_info = from_json(to_visit_bytes)
        url = to_visit_info['url']
        depth = to_visit_info['depth']
        if redis.sismember(found_key(task_id), url):
            continue
        logging.info(f"load_url:  task_id:{task_id}, visit url:{url}, root_url:{root_url},depth:{depth}, max_depth:{max_depth}, filters:{filters}")
        try:
            response = requests.get(url,timeout=(20.0, 60.0))
            content_type = response.headers.get('Content-Type')
            if response.status_code == 200 and content_type.startswith('text/html'):
                response_url = response.url
                soup = BeautifulSoup(response.text, "html.parser")
                all_links = [link.get("href") for link in soup.find_all("a")]
                for li in soup.find_all("li", attrs={'data-link': True}):
                    all_links.append(li['data-link'])

                absolute_paths = list(
                    {
                        format_link(urljoin(response_url, link))
                        for link in all_links
                    }
                )

                for link in absolute_paths:
                    # logging.info(f"sub link:{link}, visit_filter_url:{visit_filter_url},filter_url:{filter_url}")
                    if depth < max_depth and (link.startswith(root_url) or link.startswith(http_root_url)) and not redis.sismember(visited_key(task_id), link):
                        logging.info(f"add to_visit:  task_id:{task_id}, visit url:{url}, link:{link}, root_url:{root_url}, child_depth:{depth + 1}")
                        redis.rpush(to_visit_key(task_id), to_json({'url':link, 'depth': depth + 1}))
                        redis.sadd(visited_key(task_id), link)
                found = False
                if filters == []:
                    found = True
                else:
                    for filter in filters:
                        if len(filter) > 0 and (url.startswith(filter) or url.startswith(filter.replace('https://','http://'))):
                            found = True
                            break
                if found:
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
    return f"lanying-embedding:url-loader:task:{task_id}:to_visit:v2"

def visited_key(task_id):
    return f"lanying-embedding:url-loader:task:{task_id}:visited:"

def found_key(task_id):
    return f"lanying-embedding:url-loader:task:{task_id}:found"

def to_json(dict):
    return json.dumps(dict, ensure_ascii=False)
def from_json(text):
    return json.loads(text)
